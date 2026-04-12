import json
import multiprocessing as mp
import os
import shutil
import sqlite3
import sys
import tempfile
import time

N_WORKERS = 16
RUNS_PER_WORKER = 20
N_READERS = 4

_barrier = None
_guild_home = None
_runs_dir = None


def _set_globals(barrier, guild_home, runs_dir_path):
    global _barrier, _guild_home, _runs_dir
    _barrier = barrier
    _guild_home = guild_home
    _runs_dir = runs_dir_path
    os.environ["GUILD_HOME"] = guild_home


def _make_run_dir(root, run_id, status="completed", flags=None):
    run_path = os.path.join(root, run_id)
    guild_dir = os.path.join(run_path, ".guild")
    attrs_dir = os.path.join(guild_dir, "attrs")
    os.makedirs(attrs_dir, exist_ok=True)
    with open(os.path.join(guild_dir, "opref"), "w") as f:
        f.write("test:test")
    ts = str(int(time.time() * 1000000))
    with open(os.path.join(attrs_dir, "initialized"), "w") as f:
        f.write(ts)
    with open(os.path.join(attrs_dir, "started"), "w") as f:
        f.write(ts)
    if flags:
        with open(os.path.join(attrs_dir, "flags"), "w") as f:
            json.dump(flags, f)
    if status == "completed":
        with open(os.path.join(attrs_dir, "exit_status"), "w") as f:
            f.write("0")


def _writer_fn(worker_id):
    _barrier.wait()
    from guild import var as gvar
    from guild import run as runlib

    errors = []
    for i in range(RUNS_PER_WORKER):
        run_id = f"{worker_id:04d}{i:04d}{'a' * 24}"
        flags = {"n-parallel": worker_id % 4 + 1, "seed": i}
        _make_run_dir(_runs_dir, run_id, status="completed", flags=flags)
        try:
            run = runlib.Run(run_id, os.path.join(_runs_dir, run_id))
            gvar.index_register_run(run, root=_runs_dir)
            gvar.index_update_status(run, "completed", root=_runs_dir)
            gvar.index_update_attr(run, "flags", flags, root=_runs_dir)
        except Exception as e:
            errors.append(f"Writer {worker_id} run {i}: {e}")
    return errors


def _reader_fn(reader_id):
    _barrier.wait()
    from guild import var as gvar

    errors = []
    successes = 0
    end_time = time.time() + 5.0
    while time.time() < end_time:
        try:
            result = gvar.index_query_runs(root=_runs_dir)
            if result is not None:
                successes += 1
        except Exception as e:
            errors.append(f"Reader {reader_id}: {e}")
        time.sleep(0.05)
    return {"errors": errors, "successes": successes}


def _flag_filter_fn(reader_id):
    _barrier.wait()
    from guild import var as gvar
    from guild import filter as filterlib

    errors = []
    successes = 0
    end_time = time.time() + 5.0
    filter_expr = filterlib.parser().parse("n-parallel = 2")
    while time.time() < end_time:
        try:
            result = gvar.index_query_runs(root=_runs_dir, filter_expr=filter_expr)
            if result is not None:
                successes += 1
        except Exception as e:
            errors.append(f"FlagReader {reader_id}: {e}")
        time.sleep(0.05)
    return {"errors": errors, "successes": successes}


def _status_filter_fn(reader_id):
    _barrier.wait()
    from guild import var as gvar

    errors = []
    successes = 0
    end_time = time.time() + 5.0
    base_sql, base_params = gvar._compile_base_filters(status_include=["completed"])
    while time.time() < end_time:
        try:
            result = gvar.index_query_runs(
                root=_runs_dir, base_sql=base_sql, base_params=base_params
            )
            if result is not None:
                successes += 1
        except Exception as e:
            errors.append(f"StatusReader {reader_id}: {e}")
        time.sleep(0.05)
    return {"errors": errors, "successes": successes}


def _run_pool(n_procs, fn_and_args_list):
    barrier = mp.Barrier(n_procs)
    tmpdir = tempfile.mkdtemp(prefix="guild_test_")
    runs_dir_path = os.path.join(tmpdir, "runs")
    os.makedirs(runs_dir_path, exist_ok=True)

    with mp.Pool(
        n_procs,
        initializer=_set_globals,
        initargs=(barrier, tmpdir, runs_dir_path),
    ) as pool:
        async_results = [
            pool.apply_async(fn, (arg,)) for fn, arg in fn_and_args_list
        ]
        results = [r.get(timeout=120) for r in async_results]

    return tmpdir, runs_dir_path, results


def test_concurrent_writes():
    print(f"\n=== Test: {N_WORKERS} concurrent writers, {RUNS_PER_WORKER} runs each ===")

    fn_args = [(_writer_fn, i) for i in range(N_WORKERS)]
    tmpdir, runs_dir_path, results = _run_pool(N_WORKERS, fn_args)

    try:
        all_errors = []
        for errs in results:
            all_errors.extend(errs)

        if all_errors:
            print(f"  ERRORS ({len(all_errors)}):")
            for e in all_errors[:10]:
                print(f"    {e}")
        else:
            print(f"  All {N_WORKERS * RUNS_PER_WORKER} writes succeeded without errors")

        os.environ["GUILD_HOME"] = tmpdir
        for mod in list(sys.modules):
            if mod.startswith("guild"):
                sys.modules.pop(mod, None)
        from guild import var as gvar

        result = gvar.index_query_runs(root=runs_dir_path)
        if result is None:
            gvar.index_sync(root=runs_dir_path)
            result = gvar.index_query_runs(root=runs_dir_path)
        n_indexed = len(result) if result else 0
        expected = N_WORKERS * RUNS_PER_WORKER

        db_path = gvar._index_db_path(runs_dir_path)
        conn = sqlite3.connect(db_path)
        try:
            integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
            print(f"  DB integrity: {integrity}")
        finally:
            conn.close()

        passed = n_indexed == expected
        print(f"  Indexed runs: {n_indexed}/{expected} [{'PASS' if passed else 'FAIL'}]")
        return passed
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_concurrent_read_write():
    print(f"\n=== Test: {N_WORKERS} writers + {N_READERS} readers concurrently ===")

    fn_args = [(_writer_fn, i) for i in range(N_WORKERS)]
    fn_args += [(_reader_fn, i) for i in range(N_READERS)]
    tmpdir, runs_dir_path, results = _run_pool(N_WORKERS + N_READERS, fn_args)

    try:
        w_results = results[:N_WORKERS]
        r_results = results[N_WORKERS:]

        write_errors = sum(len(e) for e in w_results)
        read_errors = sum(len(r["errors"]) for r in r_results)
        read_successes = sum(r["successes"] for r in r_results)

        print(f"  Write errors: {write_errors}")
        print(f"  Read errors: {read_errors}")
        print(f"  Successful reads: {read_successes}")

        if read_errors > 0:
            for r in r_results:
                for e in r["errors"][:3]:
                    print(f"    {e}")

        passed = write_errors == 0 and read_errors == 0
        print(f"  [{'PASS' if passed else 'FAIL'}]")
        return passed
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_flag_filter_concurrent():
    print(f"\n=== Test: Flag filter (n-parallel=X) with concurrent writers ===")

    fn_args = [(_writer_fn, i) for i in range(N_WORKERS)]
    fn_args += [(_flag_filter_fn, i) for i in range(N_READERS)]
    tmpdir, runs_dir_path, results = _run_pool(N_WORKERS + N_READERS, fn_args)

    try:
        w_results = results[:N_WORKERS]
        r_results = results[N_WORKERS:]

        write_errors = sum(len(e) for e in w_results)
        read_errors = sum(len(r["errors"]) for r in r_results)
        read_successes = sum(r["successes"] for r in r_results)

        print(f"  Write errors: {write_errors}")
        print(f"  Flag filter errors: {read_errors}")
        print(f"  Successful flag filter reads: {read_successes}")

        os.environ["GUILD_HOME"] = tmpdir
        for mod in list(sys.modules):
            if mod.startswith("guild"):
                sys.modules.pop(mod, None)
        from guild import var as gvar
        from guild import filter as filterlib

        gvar.index_sync(root=runs_dir_path)
        filter_expr = filterlib.parser().parse("n-parallel = 2")
        result = gvar.index_query_runs(root=runs_dir_path, filter_expr=filter_expr)
        n_matched = len(result) if result else 0
        expected = RUNS_PER_WORKER * (N_WORKERS // 4)
        print(f"  Runs with n-parallel=2: {n_matched} (expected ~{expected})")

        passed = read_errors == 0 and n_matched > 0
        print(f"  [{'PASS' if passed else 'FAIL'}]")
        return passed
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_status_filter_concurrent():
    print(f"\n=== Test: Status filter (-Sc) with concurrent writers ===")

    fn_args = [(_writer_fn, i) for i in range(N_WORKERS)]
    fn_args += [(_status_filter_fn, i) for i in range(N_READERS)]
    tmpdir, runs_dir_path, results = _run_pool(N_WORKERS + N_READERS, fn_args)

    try:
        w_results = results[:N_WORKERS]
        r_results = results[N_WORKERS:]

        write_errors = sum(len(e) for e in w_results)
        read_errors = sum(len(r["errors"]) for r in r_results)
        read_successes = sum(r["successes"] for r in r_results)

        print(f"  Write errors: {write_errors}")
        print(f"  Status filter errors: {read_errors}")
        print(f"  Successful status filter reads: {read_successes}")

        os.environ["GUILD_HOME"] = tmpdir
        for mod in list(sys.modules):
            if mod.startswith("guild"):
                sys.modules.pop(mod, None)
        from guild import var as gvar

        gvar.index_sync(root=runs_dir_path)
        base_sql, base_params = gvar._compile_base_filters(status_include=["completed"])
        result = gvar.index_query_runs(
            root=runs_dir_path, base_sql=base_sql, base_params=base_params
        )
        n_completed = len(result) if result else 0
        expected = N_WORKERS * RUNS_PER_WORKER
        print(f"  Completed runs: {n_completed}/{expected}")

        passed = read_errors == 0 and n_completed == expected
        print(f"  [{'PASS' if passed else 'FAIL'}]")
        return passed
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    results = {}
    results["concurrent_writes"] = test_concurrent_writes()
    results["concurrent_read_write"] = test_concurrent_read_write()
    results["flag_filter"] = test_flag_filter_concurrent()
    results["status_filter"] = test_status_filter_concurrent()

    print("\n" + "=" * 50)
    print("SUMMARY:")
    all_pass = True
    for name, passed in results.items():
        tag = "PASS" if passed else "FAIL"
        print(f"  {name}: {tag}")
        if not passed:
            all_pass = False
    print("=" * 50)
    sys.exit(0 if all_pass else 1)
