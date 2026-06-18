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


def _diag_stage_task(argv):
    """Module-level (picklable for loky) single in-process stage. Returns
    (pid, error, run_id): error is None on success; run_id is the 32-char id of
    the staged run (parsed from output) so the caller can clean up exactly the
    runs it created. Surfaces failures instead of counting errored tasks as
    instant."""
    import os as _os
    import io as _io
    import re as _re
    import contextlib as _cl
    _os.environ["GUILD_NO_INDEX_WRITES"] = "1"
    err = None
    run_id = None
    buf = _io.StringIO()
    try:
        from guild.commands.main import main as guild_cli
        with _cl.redirect_stdout(buf), _cl.redirect_stderr(buf):
            guild_cli.main(args=list(argv), standalone_mode=False)
    except SystemExit as e:
        if e.code not in (0, None):
            err = "exit %r: %s" % (e.code, buf.getvalue().strip()[:300])
    except BaseException as e:
        err = "%s: %s | %s" % (type(e).__name__, str(e)[:150],
                               buf.getvalue().strip()[:200])
    m = _re.search(r"staged as ([0-9a-f]{32})", buf.getvalue())
    if m:
        run_id = m.group(1)
    return (_os.getpid(), err, run_id)


def probe_compare_exec(operation_args, trials, n_jobs):
    """Stage `trials` the same way the parallel-stager would, but three ways, to
    isolate joblib/loky overhead from the actual per-trial work:

      1. sequential, in one process (no joblib),
      2. joblib at n_jobs=1,
      3. joblib at n_jobs=`n_jobs`.

    All use the identical `guild run --stage` path (no batch copytree), in a
    throwaway GUILD_HOME on the same filesystem, in worker mode. Also reports
    distinct worker PIDs per joblib run (1 ⇒ worker reused; ~trials ⇒ a fresh
    worker per task, i.e. per-task re-import).
    """
    import joblib

    real_runs = _runs_dir()
    parent = os.path.dirname(os.path.dirname(real_runs))
    # Throwaway GUILD_HOME on the same filesystem (worker mode), exactly like
    # the --operation probe. SAFE: only this temp dir is ever written/removed;
    # real runs are never touched. (On the cluster this stages fine in loky
    # workers, as the --operation probe demonstrates; on some local setups the
    # loky rows may show staged 0 -- a local quirk, not a cluster result.)
    tmp_home = tempfile.mkdtemp(prefix=".guild_diag_", dir=parent)
    os.makedirs(os.path.join(tmp_home, "runs"), exist_ok=True)
    prev = {k: os.environ.get(k) for k in ("GUILD_HOME", "GUILD_NO_INDEX_WRITES")}
    os.environ["GUILD_HOME"] = tmp_home
    os.environ["GUILD_NO_INDEX_WRITES"] = "1"

    op = operation_args[0]
    flags = " ".join(operation_args[1:])
    argv = ("run --yes %s --stage %s" % (op, flags)).split()
    runs_dir = os.path.join(tmp_home, "runs")

    def _count():
        try:
            return sum(1 for n in os.listdir(runs_dir) if len(n) == 32)
        except OSError:
            return 0

    rep = {"trials": trials, "n_jobs": n_jobs}
    try:
        with contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            def _run_joblib(nj):
                # Force loky so even n_jobs=1 uses a real worker process
                # (joblib's default runs n_jobs=1 in-process, not exercising loky).
                c = _count()
                t0 = time.perf_counter()
                res = joblib.Parallel(n_jobs=nj, backend="loky")(
                    joblib.delayed(_diag_stage_task)(argv) for _ in range(trials))
                dt = time.perf_counter() - t0
                pids = set(p for p, _, _ in res)
                errs = [e for _, e, _ in res if e]
                return dt, len(pids), _count() - c, (errs[0] if errs else None)

            # cold (one stage) to warm the import in THIS process
            t0 = time.perf_counter()
            _diag_stage_task(argv)
            rep["cold_s"] = time.perf_counter() - t0

            # 1. sequential, in-process (warm)
            c = _count()
            t0 = time.perf_counter()
            for _ in range(trials):
                _diag_stage_task(argv)
            rep["seq_s"] = time.perf_counter() - t0
            rep["seq_staged"] = _count() - c

            # 2. loky, 1 worker
            rep["jb1_s"], rep["jb1_pids"], rep["jb1_staged"], rep["jb1_err"] = _run_joblib(1)
            # 3. loky, N workers
            rep["jbN_s"], rep["jbN_pids"], rep["jbN_staged"], rep["jbN_err"] = _run_joblib(n_jobs)
    finally:
        for k, v in prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        shutil.rmtree(tmp_home, ignore_errors=True)
    return rep


def probe_stage_profile(operation_args, warmup=3):
    """cProfile a single WARM in-process stage against the NFS, so the exact
    functions/syscalls that eat the per-trial seconds are visible.

    The first stage in the process is cold (module import over NFS); we run
    `warmup` stages first and profile a warm one, matching the per-trial cost
    that dominates a large batch. Runs in a throwaway GUILD_HOME on the same
    filesystem, in worker mode (GUILD_NO_INDEX_WRITES=1) to match real staging.
    """
    import cProfile
    import pstats
    from guild.commands.main import main as guild_cli

    real_runs = _runs_dir()
    parent = os.path.dirname(os.path.dirname(real_runs))
    tmp_home = tempfile.mkdtemp(prefix=".guild_diag_", dir=parent)
    os.makedirs(os.path.join(tmp_home, "runs"), exist_ok=True)
    prev = {k: os.environ.get(k) for k in ("GUILD_HOME", "GUILD_NO_INDEX_WRITES")}
    os.environ["GUILD_HOME"] = tmp_home
    os.environ["GUILD_NO_INDEX_WRITES"] = "1"

    op = operation_args[0]
    flags = " ".join(operation_args[1:])
    argv = ("run --yes %s --stage %s" % (op, flags)).split()

    def one_stage():
        try:
            guild_cli.main(args=list(argv), standalone_mode=False)
        except SystemExit:
            pass
        except BaseException:
            pass

    report = {}
    pr = cProfile.Profile()
    try:
        with contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            t0 = time.perf_counter()
            one_stage()
            report["cold_stage_s"] = time.perf_counter() - t0
            for _ in range(max(0, warmup - 1)):
                one_stage()
            t0 = time.perf_counter()
            pr.enable()
            one_stage()
            pr.disable()
            report["warm_stage_s"] = time.perf_counter() - t0
    finally:
        for k, v in prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        shutil.rmtree(tmp_home, ignore_errors=True)

    rows = []
    for func, (cc, nc, tt, ct, callers) in pstats.Stats(pr).stats.items():
        fn, ln, name = func
        label = "%s:%d(%s)" % (os.path.basename(fn), ln, name)
        rows.append({"tt": tt, "ct": ct, "nc": nc, "label": label, "fn": fn})
    report["rows"] = rows
    return report


def _print_profile(report):
    print("\n== Single-stage profile (warm, worker mode) ==")
    print("  cold first stage : %8.3f s   (one-time module import over NFS)"
          % report.get("cold_stage_s", 0))
    print("  warm stage (profiled): %8.3f s   <- the per-trial cost that matters"
          % report.get("warm_stage_s", 0))
    rows = report.get("rows", [])

    print("\n  Top 25 by SELF time (where the seconds are actually spent;")
    print("  built-ins like posix.stat/open/read = NFS syscall blocking):")
    print("  %10s %10s %9s  %s" % ("self_s", "cum_s", "ncalls", "function"))
    for r in sorted(rows, key=lambda r: r["tt"], reverse=True)[:25]:
        print("  %10.3f %10.3f %9s  %s"
              % (r["tt"], r["ct"], r["nc"], r["label"]))

    guild_rows = [r for r in rows if os.sep + "guild" + os.sep in r["fn"]]
    print("\n  Top 20 guild functions by CUMULATIVE time (the phase hierarchy):")
    print("  %10s %10s %9s  %s" % ("cum_s", "self_s", "ncalls", "function"))
    for r in sorted(guild_rows, key=lambda r: r["ct"], reverse=True)[:20]:
        print("  %10.3f %10.3f %9s  %s"
              % (r["ct"], r["tt"], r["nc"], r["label"]))


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
    parser.add_argument("--profile", action="store_true",
                        help="cProfile a single warm stage of --operation and "
                             "show which functions/syscalls dominate.")
    parser.add_argument("--compare-exec", action="store_true",
                        help="Stage --trials trials of --operation three ways "
                             "(sequential in-process, joblib n_jobs=1, joblib "
                             "n_jobs=N) to isolate joblib/loky overhead.")
    parser.add_argument("--warmup", type=int, default=3,
                        help="Warm-up stages before profiling (default 3).")
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

    if args.operation and not args.profile:
        op_args = args.operation.split()
        print("\n== End-to-end staging phases ==")
        print("  staging %d trials of: %s" % (args.trials, args.operation))
        rep = probe_staging(op_args, args.trials, args.n_jobs)
        print("  single sequential stage : %8.3f s" % rep.get("single_stage_s", 0))
        print("  parallel stage (%d jobs): %8.3f s  (%.1f ms/trial)"
              % (rep.get("n_jobs") or 0, rep.get("parallel_stage_s", 0),
                 rep.get("parallel_per_trial_ms", 0)))
        print("  post-stage index resync : %8.3f s" % rep.get("resync_s", 0))

    if args.profile:
        if not args.operation:
            parser.error("--profile requires --operation")
        _print_profile(probe_stage_profile(args.operation.split(), warmup=args.warmup))

    if args.compare_exec:
        if not args.operation:
            parser.error("--compare-exec requires --operation")
        n = args.n_jobs or 8
        r = probe_compare_exec(args.operation.split(), args.trials, n)
        t = r["trials"]
        print("\n== Execution comparison (%d trials, worker mode) ==" % t)
        print("  cold first stage          : %8.3f s   (one-time import)" % r["cold_s"])
        print("  sequential in-process     : %8.3f s  (%.1f ms/trial)  staged %d/%d"
              % (r["seq_s"], r["seq_s"] / t * 1e3, r["seq_staged"], t))
        print("  loky n_jobs=1             : %8.3f s  (%.1f ms/trial)  staged %d/%d  %d PID(s)"
              % (r["jb1_s"], r["jb1_s"] / t * 1e3, r["jb1_staged"], t, r["jb1_pids"]))
        if r.get("jb1_err"):
            print("      worker error: %s" % r["jb1_err"])
        print("  loky n_jobs=%-3d           : %8.3f s  (%.1f ms/trial)  staged %d/%d  %d PID(s)"
              % (r["n_jobs"], r["jbN_s"], r["jbN_s"] / t * 1e3, r["jbN_staged"], t, r["jbN_pids"]))
        if r.get("jbN_err"):
            print("      worker error: %s" % r["jbN_err"])
        print("\n  NOTE: trust a row only if it staged trials/trials; a row that")
        print("  staged fewer errored (see worker error) and its time is moot.")
        print("  If sequential << loky    -> the loky worker layer is the overhead.")
        print("  If joblib PIDs ~= trials -> loky spawns a fresh worker per task")
        print("  (per-task re-import over NFS). If PIDs are few, reuse works and")
        print("  the cost is elsewhere.")


if __name__ == "__main__":
    main()
