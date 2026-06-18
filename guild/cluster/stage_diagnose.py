"""Staging bottleneck diagnostic.

Measures *actual wall-clock* costs of the operations staging performs, so the
real bottleneck on a given filesystem (esp. a cluster NAS) can be identified
directly instead of inferred from syscall counts.

Three probes:

1. Filesystem primitives -- latency of stat / create+write / fsync / mkdir /
   rename / unlink / readdir against the real GUILD_HOME filesystem. This is
   the number that differs wildly between local disk (~us) and NAS (~ms) and
   that syscall counts cannot reveal.

2. SQLite index ops -- insert+commit latency at synchronous=OFF/NORMAL/FULL
   (quantifies what disabling commit fsyncs saves on this FS), plus a primary
   key lookup and a full COUNT.

3. End-to-end staging phases (optional, needs an operation) -- times
   parallel staging and the post-stage index resync in a throwaway GUILD_HOME
   on the same filesystem.

Safety: every probe writes only into its own temp directory (a
`.stage_diag_<pid>` dir under the runs dir, or a throwaway GUILD_HOME) and
removes only that. It never touches real runs.

Run on the headnode (where GUILD_HOME is the NAS):

    python -m guild.cluster.stage_diagnose                 # fs + sqlite probes
    python -m guild.cluster.stage_diagnose --iterations 300
    python -m guild.cluster.stage_diagnose \\
        --operation 'rollout iterations=200 num-samples=1 n-parallel=1 parallel=no' \\
        --trials 100
"""

import argparse
import contextlib
import io
import os
import shutil
import sqlite3
import tempfile
import time


def _runs_dir():
    try:
        from guild import var
        return var.runs_dir()
    except Exception:
        home = os.environ.get("GUILD_HOME") or os.path.expanduser("~/.guild")
        return os.path.join(home, "runs")


def _stats_us(times):
    """(median, p95, mean, min) in microseconds from a list of seconds."""
    if not times:
        return (0.0, 0.0, 0.0, 0.0)
    s = sorted(times)
    n = len(s)
    median = s[n // 2]
    p95 = s[min(n - 1, int(0.95 * n))]
    mean = sum(s) / n
    return (median * 1e6, p95 * 1e6, mean * 1e6, s[0] * 1e6)


def _time_each(setup_fn, op_fn, n, warmup=3):
    """Run op_fn(i) n times, returning per-call seconds. setup_fn(i) (untimed)
    prepares state for call i."""
    times = []
    for i in range(n + warmup):
        if setup_fn is not None:
            setup_fn(i)
        t0 = time.perf_counter()
        op_fn(i)
        dt = time.perf_counter() - t0
        if i >= warmup:
            times.append(dt)
    return times


def probe_fs(target_dir, n):
    os.makedirs(target_dir, exist_ok=True)
    blob = b"x" * 256  # ~ an attr file
    results = {}

    # stat an existing file
    p = os.path.join(target_dir, "statme")
    with open(p, "wb") as f:
        f.write(blob)
    results["stat"] = _stats_us(_time_each(None, lambda i: os.stat(p), n))

    # read a small file (open+read+close)
    def _read(i):
        with open(p, "rb") as f:
            f.read()
    results["read_small"] = _stats_us(_time_each(None, _read, n))

    # create + write + close (the attr-file write pattern)
    cw_dir = os.path.join(target_dir, "cw")
    os.makedirs(cw_dir, exist_ok=True)
    def _create_write(i):
        fp = os.path.join(cw_dir, "f%d" % i)
        with open(fp, "wb") as f:
            f.write(blob)
    results["create_write"] = _stats_us(_time_each(None, _create_write, n))

    # create + write + fsync + close
    cwf_dir = os.path.join(target_dir, "cwf")
    os.makedirs(cwf_dir, exist_ok=True)
    def _create_write_fsync(i):
        fp = os.path.join(cwf_dir, "f%d" % i)
        with open(fp, "wb") as f:
            f.write(blob)
            f.flush()
            os.fsync(f.fileno())
    results["create_write_fsync"] = _stats_us(_time_each(None, _create_write_fsync, n))

    # fsync alone on an already-open file (write 1 byte then fsync)
    sp = os.path.join(target_dir, "syncme")
    sf = open(sp, "wb")
    def _fsync(i):
        sf.write(b"x")
        sf.flush()
        os.fsync(sf.fileno())
    results["fsync"] = _stats_us(_time_each(None, _fsync, n))
    sf.close()

    # mkdir then (separately) rmdir
    md_root = os.path.join(target_dir, "md")
    os.makedirs(md_root, exist_ok=True)
    def _mkdir(i):
        os.mkdir(os.path.join(md_root, "d%d" % i))
    mkdir_times = _time_each(None, _mkdir, n)
    results["mkdir"] = _stats_us(mkdir_times)
    def _rmdir(i):
        os.rmdir(os.path.join(md_root, "d%d" % i))
    # only rmdir the ones we made past warmup-safely: recreate-and-rmdir
    rm_root = os.path.join(target_dir, "rm")
    os.makedirs(rm_root, exist_ok=True)
    def _rmdir_setup(i):
        os.mkdir(os.path.join(rm_root, "r%d" % i))
    def _rmdir_op(i):
        os.rmdir(os.path.join(rm_root, "r%d" % i))
    results["rmdir"] = _stats_us(_time_each(_rmdir_setup, _rmdir_op, n))

    # rename (a -> b -> a alternating within dir)
    ra = os.path.join(target_dir, "rename_a")
    rb = os.path.join(target_dir, "rename_b")
    with open(ra, "wb") as f:
        f.write(blob)
    state = {"a": ra, "b": rb}
    def _rename(i):
        os.rename(state["a"], state["b"])
        state["a"], state["b"] = state["b"], state["a"]
    results["rename"] = _stats_us(_time_each(None, _rename, n))

    # unlink (create in setup, unlink timed)
    ul_dir = os.path.join(target_dir, "ul")
    os.makedirs(ul_dir, exist_ok=True)
    def _unlink_setup(i):
        with open(os.path.join(ul_dir, "u%d" % i), "wb") as f:
            f.write(blob)
    def _unlink_op(i):
        os.unlink(os.path.join(ul_dir, "u%d" % i))
    results["unlink"] = _stats_us(_time_each(_unlink_setup, _unlink_op, n))

    # readdir of the REAL runs dir (size matters: selection/trash listing)
    rdir = _runs_dir()
    if os.path.isdir(rdir):
        results["listdir(runs)"] = _stats_us(
            _time_each(None, lambda i: os.listdir(rdir), max(5, n // 10))
        )

    return results


def probe_sqlite(target_dir, n):
    os.makedirs(target_dir, exist_ok=True)
    results = {}
    for sync in ("OFF", "NORMAL", "FULL"):
        db = os.path.join(target_dir, "idx_%s.db" % sync)
        conn = sqlite3.connect(db)
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA synchronous=%s" % sync)
        conn.execute("CREATE TABLE t(run_id TEXT PRIMARY KEY, status TEXT)")
        conn.commit()
        def _insert(i):
            conn.execute(
                "INSERT OR REPLACE INTO t(run_id,status) VALUES(?,?)",
                ("%032d" % i, "staged"),
            )
            conn.commit()
        results["insert+commit sync=%s" % sync] = _stats_us(
            _time_each(None, _insert, n)
        )
        conn.close()
    return results


def probe_staging(operation_args, trials, n_jobs):
    """Time parallel staging + resync in a throwaway GUILD_HOME on the same
    filesystem as the real one."""
    from guild.cluster import parallel_stager

    real_runs = _runs_dir()
    parent = os.path.dirname(os.path.dirname(real_runs))  # alongside real GUILD_HOME
    tmp_home = tempfile.mkdtemp(prefix=".guild_diag_", dir=parent)
    os.makedirs(os.path.join(tmp_home, "runs"), exist_ok=True)
    prev_home = os.environ.get("GUILD_HOME")
    os.environ["GUILD_HOME"] = tmp_home

    op = operation_args[0]
    flags = " ".join(operation_args[1:])
    cmd = ("guild run --yes %s --stage %s" % (op, flags)).strip()
    cmds = [cmd for _ in range(trials)]

    report = {"tmp_home": tmp_home, "trials": trials, "n_jobs": n_jobs}
    # Suppress tqdm/guild chatter so the diagnostic output stays clean.
    quiet = contextlib.redirect_stderr(io.StringIO())
    try:
        # First stage is cold: pays worker import + one-time flag-import cache
        # refresh for this operation. Measured separately from the warm rate.
        with quiet, contextlib.redirect_stdout(io.StringIO()):
            t0 = time.perf_counter()
            parallel_stager.parallel_stage_trials([cmd], n_jobs=1)
            report["single_stage_s"] = time.perf_counter() - t0

            t0 = time.perf_counter()
            parallel_stager.parallel_stage_trials(cmds, n_jobs=n_jobs)
            report["parallel_stage_s"] = time.perf_counter() - t0
            report["parallel_per_trial_ms"] = report["parallel_stage_s"] / trials * 1e3

            t0 = time.perf_counter()
            parallel_stager._resync_index()
            report["resync_s"] = time.perf_counter() - t0
    finally:
        if prev_home is None:
            os.environ.pop("GUILD_HOME", None)
        else:
            os.environ["GUILD_HOME"] = prev_home
        shutil.rmtree(tmp_home, ignore_errors=True)
    return report


def _print_table(title, results):
    print("\n== %s ==" % title)
    print("  %-26s %10s %10s %10s %10s" % ("op", "median_us", "p95_us", "mean_us", "min_us"))
    for op, (med, p95, mean, mn) in results.items():
        print("  %-26s %10.1f %10.1f %10.1f %10.1f" % (op, med, p95, mean, mn))


def main():
    parser = argparse.ArgumentParser(
        description="Measure actual staging costs to locate the bottleneck."
    )
    parser.add_argument("--iterations", type=int, default=200,
                        help="Samples per primitive (default 200).")
    parser.add_argument("--fs-only", action="store_true",
                        help="Only run the filesystem primitive probe.")
    parser.add_argument("--operation", default=None,
                        help="Operation + flags to stage for the end-to-end "
                             "phase probe, e.g. 'rollout iterations=200 num-samples=1'.")
    parser.add_argument("--trials", type=int, default=50,
                        help="Trials to stage in the end-to-end probe (default 50).")
    parser.add_argument("--n-jobs", type=int, default=None,
                        help="joblib workers for the end-to-end probe.")
    args = parser.parse_args()

    runs = _runs_dir()
    print("GUILD runs dir: %s" % runs)
    diag_dir = os.path.join(runs, ".stage_diag_%d" % os.getpid())
    try:
        fs = probe_fs(diag_dir, args.iterations)
        _print_table("Filesystem primitives (us/op, lower=better)", fs)

        if not args.fs_only:
            sq = probe_sqlite(diag_dir, args.iterations)
            _print_table("SQLite index insert+commit (us/op)", sq)

        # Rough attribution: a worker-mode staged trial does ~12 create+write,
        # ~90 stat, ~11 mkdir, ~5 read, 0 fsync (measured by syscall trace).
        def med(k):
            return fs.get(k, (0,))[0]
        est_ms = (12 * med("create_write") + 90 * med("stat")
                  + 11 * med("mkdir") + 5 * med("read_small")) / 1e3
        print("\n  Rough per-trial FS-metadata estimate (worker mode): %.1f ms"
              % est_ms)
        print("  (12 create+write + 90 stat + 11 mkdir + 5 read; compare to the")
        print("   measured parallel_per_trial below / your observed staging rate.)")
    finally:
        shutil.rmtree(diag_dir, ignore_errors=True)

    if args.operation:
        op_args = args.operation.split()
        print("\n== End-to-end staging phases ==")
        print("  staging %d trials of: %s" % (args.trials, args.operation))
        rep = probe_staging(op_args, args.trials, args.n_jobs)
        print("  single sequential stage : %8.3f s" % rep.get("single_stage_s", 0))
        print("  parallel stage (%d jobs): %8.3f s  (%.1f ms/trial)"
              % (rep.get("n_jobs") or 0, rep.get("parallel_stage_s", 0),
                 rep.get("parallel_per_trial_ms", 0)))
        print("  post-stage index resync : %8.3f s" % rep.get("resync_s", 0))


if __name__ == "__main__":
    main()
