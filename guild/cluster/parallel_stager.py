import argparse
import contextlib
import io
import multiprocessing
import os
import shlex
import subprocess
import sys
import tempfile
import threading

import joblib
from guild.cluster.helpers import yesno


def create_trial_args(args):
    operation = None
    for arg in args:
        if "=" not in arg and not arg.startswith("-"):
            operation = arg
            break
    print(f"Detected operation: {operation}")

    with tempfile.NamedTemporaryFile() as ntf:
        argv = ["run", f"--save-trials={ntf.name}"] + args
        print(repr(" ".join(["guild"] + argv)))
        # In this process rather than a `guild` subprocess: a subprocess is
        # one more cold start (Python and guild imported over what is often
        # a networked install), and doing it here leaves guild imported and
        # the op resolved for the forked staging workers to inherit.
        _set_staging_env()
        _run_guild(argv)
        import pandas as pd

        result = pd.read_csv(ntf.name)
    trial_args = [row.dropna().to_dict() for i, row in result.iterrows()]
    return operation, trial_args


def trial_args2trial_commands(operation, trial_args, tags=None):
    tag_command = "" if tags is None else " " + " ".join([f"-t {tag}" for tag in tags]) + " "
    trial_commands = []
    for trial_arg in trial_args:
        flags = " ".join([f"{k}={v}" for k, v in trial_arg.items()])
        trial_commands.append(f"guild run --yes {operation} --stage {flags} {tag_command}")
    return trial_commands


def _stage_in_process(command):
    """Run a `guild ...` command in-process instead of spawning a new guild
    process per trial.

    A fresh `guild` process pays ~150ms of fixed startup (Python imports +
    chardet charset-detection init) before doing ~10ms of actual staging
    work. By importing guild once per persistent joblib worker and invoking
    its click entrypoint directly, every trial after the first in a worker
    skips that startup. Behaviour is unchanged: same argv, same exit
    semantics (a non-zero guild exit raises, like subprocess.check_call).
    """
    _set_staging_env()
    argv = shlex.split(command)
    if argv and argv[0] == "guild":
        argv = argv[1:]
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            _run_guild(argv, command)
    except BaseException:
        sys.stderr.write(buf.getvalue())
        raise
    return command


def _set_staging_env():
    # Staging is a bulk pre-step: skip the per-trial SQLite index write (a
    # journaled transaction on one shared NFS file that every worker contends
    # on). In writes-disabled mode guild drops a per-run dirty marker instead;
    # _resync_index() folds them into the index once after staging.
    os.environ["GUILD_NO_INDEX_WRITES"] = "1"
    # Every trial stages from the same, unchanging project: let guild select
    # and read its source code files once per worker instead of once per
    # trial (a walk of the whole project tree and a read of each file).
    os.environ["GUILD_STATIC_PROJECT"] = "1"


def _run_guild(argv, command=None):
    """Runs a guild command in this process, raising CalledProcessError on a
    non-zero exit like subprocess.check_call."""
    from guild.commands.main import main as guild_cli

    try:
        # standalone_mode=False -> return instead of sys.exit on success,
        # and raise (not exit) on error, so failures surface to the caller.
        guild_cli.main(args=argv, standalone_mode=False)
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
        if code != 0:
            raise subprocess.CalledProcessError(code, command or " ".join(argv))


def parallel_stage_trials(trial_commands, n_jobs=None):
    return _run_parallel(_stage_in_process, trial_commands, n_jobs)


def _run_parallel(f, commands, n_jobs):
    """Applies f to each command across worker processes, returning the
    results in order.

    Workers are forked from this process where that is safe, so they start
    with guild already imported and the op already resolved. Spawned workers
    (joblib's loky) each start a fresh interpreter and import everything
    again, a cold start per worker that over a networked install takes tens
    of seconds. GUILD_STAGER_BACKEND=loky forces spawned workers.
    """
    if n_jobs is None:
        n_jobs = joblib.cpu_count()
    if _can_fork():
        n_jobs = joblib.effective_n_jobs(n_jobs)
        # An SQLite connection must not cross a fork; workers open their own.
        _close_index_conns()
        with multiprocessing.get_context("fork").Pool(n_jobs) as pool:
            return list(
                _maybe_progress(pool.imap(f, commands, chunksize=1), len(commands))
            )
    jobs = [joblib.delayed(f)(command) for command in commands]
    return joblib.Parallel(n_jobs=n_jobs)(_maybe_progress(jobs))


def _can_fork():
    if os.getenv("GUILD_STAGER_BACKEND", "fork") != "fork":
        return False
    if "fork" not in multiprocessing.get_all_start_methods():
        return False
    # A child forked while another thread runs inherits whatever locks that
    # thread held; only fork from a single-threaded process.
    return threading.active_count() == 1


def _close_index_conns():
    from guild import var

    for key in [k for k in vars(var._index_local) if k.startswith("conn_")]:
        var._drop_cached_conn(key)


def _maybe_progress(jobs, total=None):
    """Wrap jobs in a tqdm progress bar when tqdm is installed.

    tqdm is not a hard requirement of Guild itself, and this module is now
    imported by `guild runs restage`, which must work on an interpreter that
    only has Guild's own dependencies.
    """
    try:
        import tqdm
    except ImportError:
        return jobs
    return tqdm.tqdm(jobs, total=total)


def _stage_in_process_result(command):
    """`_stage_in_process` that reports failure instead of raising.

    A batch of stages should not be abandoned part-way because one run
    failed - the serial path warns and carries on, and this keeps that
    behaviour when the work is spread across workers.
    """
    try:
        _stage_in_process(command)
    except Exception as e:
        return command, str(e) or type(e).__name__
    return command, None


def parallel_stage_commands(commands, n_jobs=None):
    """Runs `guild` staging commands across workers, returning
    [(command, error_or_None)] without aborting the batch on a failure.
    """
    return _run_parallel(_stage_in_process_result, commands, n_jobs)


def _precompute_vcs_commit():
    """Compute the project's VCS commit once and expose it to staging workers
    via GUILD_VCS_COMMIT, so each trial reuses it instead of re-running
    `git status` (a full working-tree walk) per stage -- the dominant per-trial
    cost on a networked filesystem. The commit/status can't change during a
    staging run, so one computation is correct for all trials.
    """
    if os.environ.get("GUILD_VCS_COMMIT") or os.environ.get("NO_VCS_COMMIT") == "1":
        return
    try:
        from guild import config, op_util
        val = op_util.vcs_commit_for_dir(config.cwd())
    except Exception as e:
        print(f"VCS commit precompute skipped: {e}")
        return
    if val:
        os.environ["GUILD_VCS_COMMIT"] = val


def _resync_index(n_jobs=None):
    """Fold the per-run dirty markers left by writes-disabled staging into the
    SQLite index in a single delta sync, so the index is consistent when
    staging returns instead of resyncing lazily on the user's next command.

    Writes must be enabled here (pop the worker flag in case a non-process
    joblib backend set it in this process); opening the index with fresh
    markers present triggers the delta sync over only the staged runs.

    The sync's per-run reads run on as many threads as staging used workers,
    unless GUILD_RESYNC_WORKERS says otherwise: staging just did the same
    number of runs' worth of filesystem round-trips in parallel, and reading
    them back serially is the slow tail of a large batch.
    """
    os.environ.pop("GUILD_NO_INDEX_WRITES", None)
    set_workers = "GUILD_RESYNC_WORKERS" not in os.environ
    if set_workers:
        n = joblib.cpu_count() if n_jobs is None else joblib.effective_n_jobs(n_jobs)
        os.environ["GUILD_RESYNC_WORKERS"] = str(n)
    try:
        from guild import var
        var._get_index_conn()
    except Exception as e:
        print(f"Index resync after staging failed (will resync on next read): {e}")
    finally:
        if set_workers:
            os.environ.pop("GUILD_RESYNC_WORKERS", None)


def split_list(the_list, the_element, other=None):
    if other is None:
        other = []
    try:
        i = the_list.index(the_element)
        other.append(the_list[:i])
        return split_list(the_list[i + 1 :], the_element, other)
    except ValueError:
        other.append(the_list)
    return other


def main():
    # split arguments into "--" before after.

    split_args = split_list(sys.argv[1:], "--")
    guild_run_args = None
    stager_args = None
    if len(split_args) == 1:
        (guild_run_args,) = split_args
    elif len(split_args) == 2:
        guild_run_args, stager_args = split_args
    elif len(split_args) == 3:
        _, guild_run_args, stager_args = split_args

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument(
        "--yes",
        action="store_true",
        default=False,
        help="Stage the trials without the yes/no confirmation prompt.",
    )
    parser.add_argument("--n-jobs", type=int, default=None)
    parser.add_argument(
        "-t",
        "--tag",
        action="append",
        dest="tags",
        default=[],
        help="Add tags that should be passed to the guild run. Note that passing arguments",
    )

    for arg in guild_run_args:
        if arg.startswith("-"):
            raise parser.error(f"error {arg} flags cannot be passed to guild here.")

    pargs = parser.parse_args(stager_args)

    operation, trial_args = create_trial_args(guild_run_args)
    trial_commands = trial_args2trial_commands(operation, trial_args, tags=pargs.tags)
    print("\n".join(trial_commands))

    print(f"About to stage {len(trial_commands)} trials.")
    if not pargs.yes and not yesno("Continue?"):
        sys.exit(-1)

    if not pargs.dry_run:
        _precompute_vcs_commit()
        parallel_stage_trials(trial_commands, n_jobs=pargs.n_jobs)
        _resync_index(pargs.n_jobs)


if __name__ == "__main__":
    main()
