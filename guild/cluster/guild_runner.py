import argparse
import copy
import ctypes
import grp
import itertools
import json
import math
import os
import queue
import random
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import warnings
from collections.abc import Sequence
from contextlib import nullcontext
from fractions import Fraction

import tqdm
from guild import config
from guild.cluster import cv_util
from guild.cluster import mps_controller
from guild.cluster.helpers import yesno
from guild.cluster.sbatch_template import SlurmTemplate

# import psutil
# import atexit

libc = ctypes.CDLL("libc.so.6")


def float_to_fraction(number):
    MAX_DENOMINATOR = int(1e4)
    if isinstance(number, Fraction):
        return number
    return Fraction(number).limit_denominator(MAX_DENOMINATOR)


def is_sequence_but_not_string(obj):
    return isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray))


def deprecated_argument(type_converter, argument_name=""):
    def deprecated_argument(value):
        warnings.warn(f"The argument {argument_name} is deprecated.", DeprecationWarning)
        return type_converter(value)

    return deprecated_argument


def chunk(sequence, chunksize):
    for i in range(0, len(sequence), chunksize):
        yield sequence[i : i + chunksize]


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return itertools.zip_longest(fillvalue=fillvalue, *args)


def create_worker_kwargs(cuda_visible_devices, workers_per_gpu_ratio, number_of_subjob_workers=None, **kwargs):
    workers_per_gpu_ratio = float_to_fraction(workers_per_gpu_ratio)
    if number_of_subjob_workers is None:
        number_of_subjob_workers = len(cuda_visible_devices) * workers_per_gpu_ratio
    number_of_subjob_workers = float_to_fraction(number_of_subjob_workers)
    assert (
        number_of_subjob_workers.denominator == 1
    ), f"Subjob workers must be integer, but is {number_of_subjob_workers}"
    number_of_subjob_workers = number_of_subjob_workers.numerator
    worker_threadnums = list(range(number_of_subjob_workers))

    if len(cuda_visible_devices) > 0:
        grouped_gpus = list(
            map(list, grouper(cuda_visible_devices, workers_per_gpu_ratio.denominator)),
        )
        grouped_workers = list(grouper(worker_threadnums, workers_per_gpu_ratio.numerator))
        worker_gpu_pairs = list(zip(grouped_workers, grouped_gpus))

        worker_kwargs = []
        for workers, gpus in worker_gpu_pairs:
            for worker, gpus in list(zip(workers, [gpus] * len(workers))):
                if len(gpus) == 1:
                    (gpus,) = gpus
                worker_kwargs.append(dict(gpus=gpus, thread_num=worker, **kwargs))
    else:
        worker_kwargs = []
        for worker in worker_threadnums:
            worker_kwargs.append(dict(gpus=[], thread_num=worker, **kwargs))
    return worker_kwargs


def set_pdeathsig(sig=signal.SIGTERM):
    def callable():
        return libc.prctl(1, sig)

    return callable


def is_in_singularity():
    if not os.environ.get("SINGULARITY_COMMAND", ""):
        return False
    else:
        return True


def with_singularity(command):
    singularity_container = os.environ["SINGULARITY_CONTAINER"]
    if os.environ["SINGULARITY_BIND"]:
        singularity_bind = "-B " + os.environ["SINGULARITY_BIND"]
    else:
        singularity_bind = ""
    # note: the --nv nvidia extensions warn when nvidia tools are
    # not present, but does not fail
    path = os.environ["PATH"]
    shlex_command = shlex.quote(command)
    cmd = f"singularity exec --env PATH={path} --nv {singularity_bind} {singularity_container} /usr/bin/bash -i -c {shlex_command}"
    return cmd


def get_user_and_group():
    user = os.getlogin()
    group = grp.getgrgid(os.getgid()).gr_name
    return user, group


# #@atexit.register
# def kill_children(sig=signal.SIGINT):
#     print("atexit!")
#     proc = psutil.Process(os.getpid())
#     print(f"children: {proc.children()}")
#     for child in proc.children():
#         pgrp = os.getpgid(child.pid)
#         try:
#             os.killpg(pgrp, sig)
#         except Exception:
#             pass
#         os.kill(child.pid, sig)
#     try:
#         os.killpg(proc.pid, sig)
#     except Exception:
#         pass


def resolve_cuda_visible_devices(number_of_gpus):
    """Read CUDA_VISIBLE_DEVICES from the environment and validate it against the
    number of GPUs the job requested."""
    CUDA_VISIBLE_DEVICES_str = (
        re.sub(",+", ",", os.environ.get("CUDA_VISIBLE_DEVICES", "").replace(" ", ","))
    ).strip()
    if number_of_gpus and number_of_gpus > 0:
        assert CUDA_VISIBLE_DEVICES_str, f"CUDA_VISIBLE_DEVICES does not show devices: {CUDA_VISIBLE_DEVICES_str}"
    cuda_visible_devices = CUDA_VISIBLE_DEVICES_str.split(",") if CUDA_VISIBLE_DEVICES_str else []

    if number_of_gpus is not None:
        if len(cuda_visible_devices) != number_of_gpus:
            raise RuntimeError(f"expected {number_of_gpus} required, but {len(cuda_visible_devices)} available")
    return cuda_visible_devices


class Worker(object):
    def __init__(self, gpus, thread_num, queue, dry_run=False):
        if is_sequence_but_not_string(gpus):
            gpus = ",".join(gpus)
        self.gpus = gpus
        self.queue = queue
        self.thread_num = thread_num
        self.dry_run = dry_run

    def do_work(self):
        while True:
            item = self.queue.get()  # timeout=0.01)
            runid = item["id"]
            print(f"Working on {runid}")
            env = copy.deepcopy(os.environ)
            env["CUDA_VISIBLE_DEVICES"] = self.gpus
            try:
                print(f"guild run -y --restart {runid}")
                if not self.dry_run:
                    subprocess.run(
                        shlex.split(f"guild run -y --restart {runid}"),
                        preexec_fn=set_pdeathsig(signal.SIGINT),
                    )
                else:
                    print("(dryrun)")
                print(f"Finished {runid}")
            except Exception:
                traceback.print_exc()
            finally:
                self.queue.task_done()
            time.sleep(0.1)


# filter store and read runs.
class Runs:
    @staticmethod
    def guild_read(runsfilter="-Se"):
        guild_command = f"guild runs --json {runsfilter}"
        print(f"guild_command: {guild_command}")
        output = subprocess.check_output(guild_command, shell=True)
        runs = json.loads(output)
        return runs

    @staticmethod
    def bare_ids_to_json(list_of_ids):
        return [dict(id=runid) for runid in list_of_ids]

    @staticmethod
    def read_json(filename):
        with open(filename, "r") as fobj:
            return json.load(fobj)

    @staticmethod
    def store_json(runs, filename):
        assert runs
        with open(filename, "w") as fobj:
            json.dump(runs, fobj)

    @staticmethod
    def execute(runs, number_of_subjob_workers=None, dry_run=False, use_mps=False, number_of_gpus=None):
        """
        This function executes a given list of runs.
        """
        workers_per_gpu = number_of_subjob_workers / max(number_of_gpus, 1)  # even if there are zero gpus,
        # set ratio to 1.

        cuda_visible_devices = resolve_cuda_visible_devices(number_of_gpus)

        run_queue = queue.Queue()
        for run in runs:
            run_queue.put(run)

        with mps_controller.make_mps_controller() if use_mps else nullcontext() as mps:
            if use_mps:
                os.environ.update(mps.get_env_keys())

            worker_kwargs = create_worker_kwargs(
                cuda_visible_devices=cuda_visible_devices,
                workers_per_gpu_ratio=workers_per_gpu,
                number_of_subjob_workers=number_of_subjob_workers,
                queue=run_queue,
                dry_run=dry_run,
            )
            workers = [Worker(**kwargs) for kwargs in worker_kwargs]
            threads = [threading.Thread(target=worker.do_work, daemon=True).start() for worker in workers]

            # block until all tasks are done
            time.sleep(3)
            print("-------\n" * 10)
            print(f"PID: {os.getpid()}")
            print("Waiting for queue")
            run_queue.join()
            print("All work completed")
            threads.clear()


class PoolWorker(object):
    """A worker that drains a shared, filesystem-backed run queue.

    Many of these run concurrently across the tasks of a single SLURM job array.
    Each one walks the manifest of run ids in its own random order and claims a
    run by atomically creating a marker directory under ``claims_dir`` (os.mkdir
    is atomic across NFS/Lustre/GPFS, so exactly one worker wins each run). This
    gives dynamic load balancing: a worker stuck on a slow run simply claims
    fewer runs, instead of a fixed chunk stalling on its hardest member.

    Reliability (kept deliberately light, no heartbeats/leases/within-run retry):
    - A run is only marked done (``done_dir``) after ``guild run`` exits 0. A run
      that fails or whose worker is killed mid-run keeps its claim and gets no
      done marker, so within one submission it is attempted exactly once (the held
      claim stops other workers from re-running it -> no duplicates, no retry
      storms).
    - Recovery is uniform and deterministic: resubmit the persisted submit script.
      The claim is tagged with the array generation (SLURM_ARRAY_JOB_ID), so a new
      submission re-claims every not-done run -- whether it failed cleanly or its
      worker crashed -- and attempts each exactly once again. Loop the resubmit
      until the done count stops growing to ride out transient failures.
    """

    def __init__(self, gpus, thread_num, todo_ids, claims_dir, done_dir, generation, dry_run=False):
        if is_sequence_but_not_string(gpus):
            gpus = ",".join(gpus)
        self.gpus = gpus
        self.thread_num = thread_num
        self.todo_ids = todo_ids
        self.claims_dir = claims_dir
        self.done_dir = done_dir
        # The claim marker is tagged with the array's generation (SLURM_ARRAY_JOB_ID),
        # so a fresh submission re-claims any not-done run even if a worker from a
        # previous generation died mid-run and left its claim behind; within one
        # generation the per-run name is shared, so the claim is still exactly-once.
        self.generation = generation
        self.dry_run = dry_run

    def _claim_path(self, runid):
        return os.path.join(self.claims_dir, f"{runid}.{self.generation}")

    def _claim(self, runid):
        try:
            os.mkdir(self._claim_path(runid))
            return True
        except FileExistsError:
            return False

    def _mark_done(self, runid):
        try:
            os.mkdir(os.path.join(self.done_dir, runid))
        except FileExistsError:
            pass

    def _run(self, runid, env):
        print(f"[worker {self.thread_num}] claimed {runid}")
        print(f"guild run -y --restart {runid}")
        if self.dry_run:
            print("(dryrun)")
            return True
        try:
            result = subprocess.run(
                shlex.split(f"guild run -y --restart {runid}"),
                env=env,
                preexec_fn=set_pdeathsig(signal.SIGINT),
            )
        except Exception:
            traceback.print_exc()
            return False
        if result.returncode != 0:
            print(f"[worker {self.thread_num}] run {runid} FAILED (rc={result.returncode}); left not-done for resubmit")
            return False
        print(f"[worker {self.thread_num}] finished {runid}")
        return True

    def do_work(self):
        env = copy.deepcopy(os.environ)
        env["CUDA_VISIBLE_DEVICES"] = self.gpus
        suffix = f".{self.generation}"
        while True:
            # Snapshot done + currently-claimed (two listdirs) so we only attempt
            # mkdir on ids that look free; this keeps the common path at ~1 mkdir
            # per run instead of one mkdir per id in the manifest. mkdir still
            # arbitrates the race, so the snapshot only needs to be a good hint.
            done_set = set(os.listdir(self.done_dir))
            claimed_set = set(os.listdir(self.claims_dir))
            candidates = [
                runid
                for runid in self.todo_ids
                if runid not in done_set and (runid + suffix) not in claimed_set
            ]
            if not candidates:
                break  # every run is done or already claimed in this generation
            random.shuffle(candidates)
            ran_one = False
            for runid in candidates:
                if not self._claim(runid):
                    continue  # lost the race to a peer; try the next candidate
                ran_one = True
                if self._run(runid, env):
                    self._mark_done(runid)
                # On failure the claim is intentionally kept: the run stays not-done
                # and is left for a resubmit (new generation), never re-run here.
                break  # re-snapshot so the next candidate set stays small and fresh
            # Every candidate we saw was claimed by a peer before we could -> the
            # queue is drained for this generation; stop without idle-waiting.
            if not ran_one:
                break


def execute_shared_queue(queue_dir, number_of_subjob_workers=None, dry_run=False, use_mps=False, number_of_gpus=None):
    """Drain the shared run queue at ``queue_dir`` using a pool of PoolWorkers.

    Invoked once per SLURM array task; all tasks point at the same ``queue_dir``
    and coordinate purely through atomic claim directories, so no task needs to
    know its array index.
    """
    manifest_path = os.path.join(queue_dir, "manifest")
    claims_dir = os.path.join(queue_dir, "claims")
    done_dir = os.path.join(queue_dir, "done")
    os.makedirs(claims_dir, exist_ok=True)
    os.makedirs(done_dir, exist_ok=True)
    with open(manifest_path, "r") as fobj:
        todo_ids = [line.strip() for line in fobj if line.strip()]

    # Shared across all tasks of one submission (so claims are exactly-once), but
    # changes on resubmit (so a resubmit can reclaim runs left behind by a crash).
    generation = os.environ.get("SLURM_ARRAY_JOB_ID", "0")

    workers_per_gpu = number_of_subjob_workers / max(number_of_gpus, 1)
    cuda_visible_devices = resolve_cuda_visible_devices(number_of_gpus)

    with mps_controller.make_mps_controller() if use_mps else nullcontext() as mps:
        if use_mps:
            os.environ.update(mps.get_env_keys())

        worker_kwargs = create_worker_kwargs(
            cuda_visible_devices=cuda_visible_devices,
            workers_per_gpu_ratio=workers_per_gpu,
            number_of_subjob_workers=number_of_subjob_workers,
            todo_ids=todo_ids,
            claims_dir=claims_dir,
            done_dir=done_dir,
            generation=generation,
            dry_run=dry_run,
        )
        workers = [PoolWorker(**kwargs) for kwargs in worker_kwargs]
        threads = [threading.Thread(target=worker.do_work) for worker in workers]
        print(f"PID: {os.getpid()} draining shared queue '{queue_dir}' ({len(todo_ids)} runs) with {len(workers)} worker(s)")
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        print("All runs in shared queue claimed; this task's worker(s) are done.")


def main():
    parser = argparse.ArgumentParser(
        description="select and schedule guild runs on a slurm cluster.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    group_runsin = parser.add_mutually_exclusive_group()
    group_runsin.add_argument("--guildfilter", type=str, default=None, help="filter string for guild runs")
    group_runsin.add_argument("--runsfile", type=str, default=None, help="json file result of guild runs")
    group_runsin.add_argument(
        "--runids",
        nargs="+",
    )

    parser.add_argument("--store-runs", type=str, default=None, help="filename to write filtered runs to")

    group_execute = parser.add_mutually_exclusive_group()
    group_execute.add_argument("--sbatch", action="store_true")
    parser.add_argument("--sbatch-yes", action="store_true")
    parser.add_argument("--sbatch-verbose", action="store_true")
    parser.add_argument(
        "--convert-cuda-visible-uuids", action="store_true", help="Use nvidia-smi to convert visible devices to uuids."
    )
    parser.add_argument("--use-mps", action="store_true", help="Should an nvidia-cuda-mps-control daemon be launched?")
    group_execute.add_argument("--exec", action="store_true")

    # parser.add_argument("--jobs-per-gpu", type=int, default=5)
    parser.add_argument(
        "--workers-per-job",
        type=int,
        default=1,
        help=("how many workers per slurm job. " "These will be distributed evenly across GPUs or vice versa."),
    )
    parser.add_argument("--dry-run", action="store_true")

    parser.add_argument("--partition", type=str, default="IFIgpu")
    parser.add_argument("--exclude-nodes", type=str, default="headnode")
    parser.add_argument("--guild-home", type=str, default=None, help="GUILD_HOME directory")

    # sbatch additional parameters
    parser.add_argument(
        "--create-template", type=str, required=False, help="Create a template (choose template from a list)"
    )
    parser.add_argument(
        "--template-file",
        type=str,
        default=SlurmTemplate.get_default_template_filename(),
        help="Path to sbatch (string.Template) template",
    )
    parser.add_argument("--list-templates", action="store_true")
    parser.add_argument("--jobname", type=str, default="guild-runner")
    parser.add_argument("--nice", type=int, default=0)
    parser.add_argument("--use-jobs", type=int, default=-1, help="how many parallel sbatch files and thus jobs to use")
    parser.add_argument(
        "--job-array",
        action="store_true",
        help=(
            "Submit all chunks as a single SLURM job array (one job id, indexed tasks) "
            "instead of one independent sbatch job per chunk. Each task runs a fixed "
            "chunk of runs. Requires --sbatch."
        ),
    )
    parser.add_argument(
        "--shared-queue",
        action="store_true",
        help=(
            "Submit one SLURM job array whose tasks dynamically pull runs from a shared "
            "queue (atomic claim per run) instead of getting a fixed chunk. Best when run "
            "times vary a lot, since slow runs don't stall a whole chunk. Number of tasks = "
            "--use-jobs (set this to your max concurrency). Requires --sbatch; "
            "mutually exclusive with --job-array."
        ),
    )
    parser.add_argument(
        "--queue-dir",
        type=str,
        default=None,
        help=(
            "Directory on shared storage for the --shared-queue manifest and claim markers "
            "(default: $GUILD_HOME/_slurm_queue). Must be readable from the compute nodes "
            "(and bind-mounted into the container when using singularity)."
        ),
    )
    parser.add_argument(
        "--shared-queue-dir",
        type=str,
        default=None,
        help=argparse.SUPPRESS,  # internal: passed to --exec pull-worker tasks
    )
    parser.add_argument("--num-gpus", type=int, default=1, help="How many GPUs to request via slurm. Minimum is 1.")
    parser.add_argument("--num-cpus", type=int, default=1, help="How many CPUs per job.")
    parser.add_argument(
        "--delay-start",
        type=float,
        default=0.1,
        help="How many seconds to wait between launching slurm jobs, defaults to 0.1",
    )

    args = parser.parse_args()

    if args.job_array and not args.sbatch:
        parser.error("--job-array requires --sbatch")

    if args.shared_queue and not args.sbatch:
        parser.error("--shared-queue requires --sbatch")

    if args.shared_queue and args.job_array:
        parser.error("--shared-queue and --job-array are mutually exclusive")

    if args.use_mps and not args.convert_cuda_visible_uuids:
        print("NOTE: '--use-mps' implies  '--convert-cuda-visible-uuids'")
        args.convert_cuda_visible_uuids = True

    if args.guild_home:
        os.environ["GUILD_HOME"] = args.guild_home
        guild_home = f"export GUILD_HOME='{args.guild_home}'"
    else:
        guild_home = ""

    # sbatch templates
    if args.list_templates:
        SlurmTemplate.print_defaults()
        parser.exit("")

    if args.create_template:
        SlurmTemplate.create_template(args.template_file, args.create_template)
        SlurmTemplate.print_template(args.create_template)
    if args.sbatch:
        slurm_template = SlurmTemplate(args.template_file)

    # ---- read runs...
    runs = None
    if args.guildfilter:
        runs = Runs.guild_read(args.guildfilter)
    elif args.runids:
        runs = Runs.bare_ids_to_json(args.runids)
    elif args.runsfile:
        runs = Runs.read_json(args.runsfile)

    if args.store_runs:
        Runs.store_json(runs, args.store_runs)
    # ----

    # sbatch or execute:
    if args.exec:
        if args.convert_cuda_visible_uuids:
            cv_util.convert_cuda_visible_devices(os.environ)
        if args.shared_queue_dir:
            print("execute runs from shared queue!")
            execute_shared_queue(
                args.shared_queue_dir,
                args.workers_per_job,
                dry_run=args.dry_run,
                use_mps=args.use_mps,
                number_of_gpus=args.num_gpus,
            )
        else:
            print("execute runs!")
            Runs.execute(
                runs, args.workers_per_job, dry_run=args.dry_run, use_mps=args.use_mps, number_of_gpus=args.num_gpus
            )
    elif args.sbatch:
        print("should create sbatch...")

        flags_passthrough = []
        if args.use_mps:
            flags_passthrough.append("--use-mps")
        if args.convert_cuda_visible_uuids:
            flags_passthrough.append("--convert-cuda-visible-uuids")
        flags_passthrough_string = " " + " ".join(flags_passthrough) + " "
        username, groupname = get_user_and_group()

        if args.shared_queue:
            nr_of_runs = len(runs)
            n_workers = args.use_jobs
            if n_workers < 1:
                parser.error(
                    "--shared-queue requires --use-jobs N (the number of pull-worker "
                    "tasks, e.g. your max concurrent jobs)"
                )
            print(f"shared queue: {nr_of_runs} runs drained by {n_workers} pull-worker task(s)")
            if not args.sbatch_yes:
                if not yesno("Continue?"):
                    sys.exit(-1)

            # Persist the manifest + claim/done dirs on shared storage; the array
            # tasks read these at run time, possibly long after submission.
            # Default under GUILD_HOME: it is on the same shared storage as the runs
            # and already bind-mounted into the container, so the array tasks can
            # always read the queue (cwd may be neither).
            base_dir = args.queue_dir if args.queue_dir else os.path.join(config.guild_home(), "_slurm_queue")
            os.makedirs(base_dir, exist_ok=True)
            queue_dir = tempfile.mkdtemp(prefix=f"guild-slurm-queue-{args.jobname}-", dir=base_dir)
            os.makedirs(os.path.join(queue_dir, "claims"), exist_ok=True)
            os.makedirs(os.path.join(queue_dir, "done"), exist_ok=True)
            # Shuffle so the manifest order itself is random: combined with each
            # worker's private shuffle, hard runs are spread across tasks rather
            # than clustered.
            shuffled_ids = [run["id"] for run in runs]
            random.shuffle(shuffled_ids)
            with open(os.path.join(queue_dir, "manifest"), "w") as fobj:
                fobj.write("\n".join(shuffled_ids) + "\n")
            print(f"shared queue dir: {queue_dir}")

            command = (
                f"{sys.executable} {__file__} --exec {flags_passthrough_string}"
                f" --shared-queue-dir {shlex.quote(queue_dir)}"
                f" --workers-per-job {args.workers_per_job} --num-gpus {args.num_gpus} --num-cpus {args.num_cpus}"
            )
            if is_in_singularity():
                command = with_singularity(command)
            slurm_content = slurm_template.substitute(
                user=username,
                cmd=command,
                jobname=args.jobname,
                guild_home=guild_home,
                num_gpus=args.num_gpus,
                num_cores=args.num_cpus,
                partition=args.partition,
                exclude_nodes=args.exclude_nodes,
                group=groupname,
            )
            # Keep the submit script next to the queue so the run can be resumed
            # after failures: resubmitting it re-runs only the not-yet-done runs.
            submit_path = os.path.join(queue_dir, "submit.sh")
            with open(submit_path, "w") as sbash:
                sbash.write(slurm_content)
            sbatch_command = f"sbatch --array=0-{n_workers - 1} --nice={args.nice} {shlex.quote(submit_path)} "
            if args.sbatch_verbose:
                print(f"\n--- sbatch shared-queue file, {submit_path} ---\n")
                subprocess.run(f"cat {shlex.quote(submit_path)}", shell=True)
            print(f"command: {sbatch_command}")
            if not args.dry_run:
                result = subprocess.run(sbatch_command, shell=True)
                if result.returncode != 0:
                    raise RuntimeError(f"sbatch failed (rc={result.returncode}); queue left at {queue_dir}")
            print(f"\n=== submitted 1 job array ({n_workers} pull-worker tasks) draining {nr_of_runs} runs ===")
            print(f"resume after failures by resubmitting: {sbatch_command.strip()}\n")
            return

        nr_of_runs = len(runs)
        worker_slots_per_slurmjob = args.workers_per_job
        frac_num_jobs = nr_of_runs / worker_slots_per_slurmjob
        full_jobs = int(math.ceil(frac_num_jobs))
        nr_of_jobs = args.use_jobs
        print(f"num jobs: {nr_of_runs}")
        print(f"workers per slurmjob: {worker_slots_per_slurmjob}")
        if args.num_gpus > 0:
            if args.num_gpus > worker_slots_per_slurmjob:
                print(f"gpus per worker: {float_to_fraction(args.num_gpus / worker_slots_per_slurmjob)}")
                print("Using Multiple-GPUs")
                if args.num_gpus % worker_slots_per_slurmjob != 0:
                    raise RuntimeError(
                        f"Requesting {args.num_gpus} GPUs, and {worker_slots_per_slurmjob} subjobs:"
                        " GPUs cannot be distributed evenly among subjobs."
                    )
            else:
                print(f"workers per gpu: {float_to_fraction(worker_slots_per_slurmjob / args.num_gpus)}")
                print("Running parallel jobs on GPU.")
        print(f"requested cpus: {args.num_cpus}")
        if args.use_jobs > full_jobs:
            raise RuntimeError(
                (
                    f"We have {nr_of_runs} runs, {args.num_gpus} GPUs, "
                    f"and {worker_slots_per_slurmjob} workers/slots per slurm job. "
                    f"{args.use_jobs} are requested, but we can fill only {frac_num_jobs} "
                    f"({full_jobs}) slurm jobs. Use less workers per slurm-job or less slurm-jobs."
                )
            )
        if nr_of_jobs < 1:
            over_count = int(abs(args.use_jobs))
            nr_of_jobs = int(math.ceil((nr_of_runs / worker_slots_per_slurmjob) / over_count))
            print(
                (
                    f"Automatic node calculation used, every node should execute {over_count}"
                    f" jobs sequentially, thus: {nr_of_jobs} jobs"
                )
            )

        # Chunking
        nr_of_jobs_per_node = int(math.ceil(nr_of_runs / nr_of_jobs))
        chunks = list(chunk(runs, nr_of_jobs_per_node))
        joblens = ", ".join([str(len(chunk_runs)) for chunk_runs in chunks])
        print(f"jobs per node: [ {joblens} ]")
        if args.job_array:
            print(f"Submitting a single SLURM job array with {len(chunks)} tasks.")

        if not args.sbatch_yes:
            if not yesno("Continue?"):
                sys.exit(-1)

        if args.job_array:
            # Each array task selects its own chunk of run ids via $SLURM_ARRAY_TASK_ID.
            # The chunks are embedded directly in the script as a bash array, so no
            # extra files need to persist on shared storage until the tasks run.
            chunk_runid_strings = [" ".join(run["id"] for run in chunk_runs) for chunk_runs in chunks]
            bash_array_body = "\n".join(shlex.quote(s) for s in chunk_runid_strings)
            array_preamble = (
                "RUNID_CHUNKS=(\n"
                f"{bash_array_body}\n"
                ")\n"
                'export CHUNK_RUNIDS="${RUNID_CHUNKS[$SLURM_ARRAY_TASK_ID]}"\n'
                'echo "array task ${SLURM_ARRAY_TASK_ID}: ${CHUNK_RUNIDS}"\n'
            )
            inner_command = (
                f"{sys.executable} {__file__} --exec {flags_passthrough_string} --runids $CHUNK_RUNIDS"
                f" --workers-per-job {args.workers_per_job} --num-gpus {args.num_gpus} --num-cpus {args.num_cpus}"
            )
            if is_in_singularity():
                # CHUNK_RUNIDS is exported above, so it is inherited inside the container.
                inner_command = with_singularity(inner_command)
            command = array_preamble + inner_command
            slurm_content = slurm_template.substitute(
                user=username,
                cmd=command,
                jobname=args.jobname,
                guild_home=guild_home,
                num_gpus=args.num_gpus,
                num_cores=args.num_cpus,
                partition=args.partition,
                exclude_nodes=args.exclude_nodes,
                group=groupname,
            )
            with tempfile.NamedTemporaryFile(mode="w", suffix=".sh") as sbash:
                sbash.write(slurm_content)
                sbash.flush()
                sbatch_command = f"sbatch --array=0-{len(chunks) - 1} --nice={args.nice} {sbash.name} "
                if args.sbatch_verbose:
                    print(f"\n--- sbatch array file, {sbash.name} ---\n")
                    subprocess.run(f"cat {sbash.name}", shell=True)
                print(f"command: {sbatch_command}")
                if not args.dry_run:
                    subprocess.run(sbatch_command, shell=True)
            print(f"\n=== submitted 1 job array with {len(chunks)} tasks ===\n")
            return

        for i, chunk_runs in tqdm.tqdm(list(enumerate(chunks))):
            chunk_runids = [run["id"] for run in chunk_runs]
            command = f"{sys.executable} {__file__} --exec {flags_passthrough_string} --runids {' '.join(chunk_runids)} --workers-per-job {args.workers_per_job} --num-gpus {args.num_gpus} --num-cpus {args.num_cpus}"
            if is_in_singularity():
                command = with_singularity(command)
            slurm_content = slurm_template.substitute(
                user=username,
                cmd=command,
                jobname=f"{args.jobname}-{i}",
                guild_home=guild_home,
                num_gpus=args.num_gpus,
                num_cores=args.num_cpus,
                partition=args.partition,
                exclude_nodes=args.exclude_nodes,
                group=groupname,
            )
            with tempfile.NamedTemporaryFile(mode="w", suffix=".sh") as sbash:
                sbash.write(slurm_content)
                sbash.flush()
                command = f"sbatch --nice={args.nice} {sbash.name} "
                if args.sbatch_verbose:
                    print(f"\n--- sbatch file for job {i}, {sbash.name} ---\n")
                    subprocess.run(f"cat {sbash.name}", shell=True)
                print(f"command: {command}")
                if not args.dry_run:
                    subprocess.run(command, shell=True)
                pass  # create batch with runs here.
            print(f"waiting {args.delay_start} seconds.", end="")
            time.sleep(args.delay_start)
            print("\r                                            \r")
        print(f"\n=== {i+1} job files===\n")


if __name__ == "__main__":
    main()
