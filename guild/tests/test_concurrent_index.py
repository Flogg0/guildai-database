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


def test_transient_error_no_nuke():
    print(f"\n=== Test: Transient DatabaseError does not nuke the index ===")
    tmpdir = tempfile.mkdtemp(prefix="guild_test_")
    runs_dir_path = os.path.join(tmpdir, "runs")
    os.makedirs(runs_dir_path, exist_ok=True)
    os.environ["GUILD_HOME"] = tmpdir
    for mod in list(sys.modules):
        if mod.startswith("guild"):
            sys.modules.pop(mod, None)
    from guild import var as gvar
    from guild import run as runlib

    try:
        # Seed with a few runs.
        for i in range(10):
            run_id = f"{i:032d}"
            _make_run_dir(runs_dir_path, run_id, status="completed",
                          flags={"seed": i})
            run = runlib.Run(run_id, os.path.join(runs_dir_path, run_id))
            gvar.index_register_run(run, root=runs_dir_path)
        gvar.index_sync(root=runs_dir_path)
        db_path = gvar._index_db_path(runs_dir_path)
        assert os.path.exists(db_path), "DB should exist after sync"
        size_before = os.path.getsize(db_path)

        # Simulate a transient DatabaseError: the retry path should NOT nuke.
        call_count = [0]
        def flaky_write():
            call_count[0] += 1
            if call_count[0] == 1:
                raise sqlite3.DatabaseError("disk I/O error (simulated)")
            # Second call: real no-op write to prove retry works.
            conn = gvar._get_index_conn(runs_dir_path)
            conn.execute("SELECT 1 FROM runs LIMIT 1")

        gvar._index_safe_write(flaky_write, root=runs_dir_path)
        assert os.path.exists(db_path), "DB should NOT be nuked on transient error"
        size_after = os.path.getsize(db_path)
        assert size_after >= size_before, "DB should not shrink"
        assert call_count[0] == 2, f"Expected 2 calls (retry), got {call_count[0]}"

        # Confirmed corruption SHOULD nuke.
        def corrupt_write():
            raise sqlite3.DatabaseError("database disk image is malformed")
        gvar._index_safe_write(corrupt_write, root=runs_dir_path)
        assert not os.path.exists(db_path), (
            "DB SHOULD be nuked on confirmed corruption"
        )
        print(f"  [PASS]")
        return True
    except AssertionError as e:
        print(f"  [FAIL] {e}")
        return False
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_filter_speed_10k():
    print(f"\n=== Test: Filter query speed with 10k runs ===")
    tmpdir = tempfile.mkdtemp(prefix="guild_test_")
    runs_dir_path = os.path.join(tmpdir, "runs")
    os.makedirs(runs_dir_path, exist_ok=True)
    os.environ["GUILD_HOME"] = tmpdir
    for mod in list(sys.modules):
        if mod.startswith("guild"):
            sys.modules.pop(mod, None)
    from guild import var as gvar
    from guild.filter import parser

    try:
        N = 10000
        t0 = time.time()
        for i in range(N):
            run_id = f"{i:032x}"
            _make_run_dir(runs_dir_path, run_id, status="completed",
                          flags={"n-parallel": i % 5, "seed": i})
        print(f"  Created {N} runs in {time.time()-t0:.2f}s")

        # Cold sync (first query on empty DB).
        t0 = time.time()
        result = gvar.index_query_runs(root=runs_dir_path, filter_expr=None)
        t_cold = time.time() - t0
        print(f"  Cold query (full sync): {len(result)} rows in {t_cold:.2f}s")
        assert len(result) == N, f"Expected {N} rows, got {len(result)}"

        # Warm query (cached conn).
        fexpr = parser().parse("n-parallel = 3")
        t0 = time.time()
        result = gvar.index_query_runs(root=runs_dir_path, filter_expr=fexpr)
        t_warm = time.time() - t0
        print(f"  Warm filter query: {len(result)} rows in {t_warm*1000:.1f}ms")
        assert t_warm < 1.0, f"Warm query too slow: {t_warm:.2f}s (want <1s)"
        assert len(result) == N // 5, f"Expected {N//5} rows, got {len(result)}"

        # Fresh-process simulation: marker exists, no sync.
        import threading as _th
        gvar._index_local = _th.local()
        t0 = time.time()
        result = gvar.index_query_runs(root=runs_dir_path, filter_expr=fexpr)
        t_fresh = time.time() - t0
        print(f"  Fresh-process filter: {len(result)} rows in {t_fresh*1000:.1f}ms")
        assert t_fresh < 1.0, (
            f"Fresh-process query too slow: {t_fresh:.2f}s (want <1s). "
            "The sync marker may not be preventing full re-sync."
        )

        print(f"  [PASS]")
        return True
    except AssertionError as e:
        print(f"  [FAIL] {e}")
        return False
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _hold_index_lock(runs_dir_path, hold_timeout=30):
    """Spawn a daemon thread that holds the index filelock. Returns
    (release_event, thread). Caller sets release_event to let it go.
    """
    import filelock as fl
    import threading
    from guild import var as gvar
    lock_path = gvar._index_db_path(runs_dir_path) + ".lock"
    acquired = threading.Event()
    release = threading.Event()

    def _hold():
        lk = fl.FileLock(lock_path, timeout=hold_timeout)
        with lk:
            acquired.set()
            release.wait(timeout=hold_timeout)

    t = threading.Thread(target=_hold, daemon=True)
    t.start()
    if not acquired.wait(timeout=10):
        raise RuntimeError("holder thread failed to acquire lock")
    return release, t, lock_path


def test_find_runs_fs_fallback_under_lock():
    """Failure A repro: find_runs must locate a run via FS scan when the
    index lock is held by another process."""
    print(f"\n=== Test: find_runs FS fallback under held lock ===")
    import filelock
    tmpdir = tempfile.mkdtemp(prefix="guild_test_")
    runs_dir_path = os.path.join(tmpdir, "runs")
    os.makedirs(runs_dir_path, exist_ok=True)
    os.environ["GUILD_HOME"] = tmpdir
    for mod in list(sys.modules):
        if mod.startswith("guild"):
            sys.modules.pop(mod, None)
    from guild import var as gvar
    from guild import run as runlib

    try:
        run_id = "a" * 32
        _make_run_dir(runs_dir_path, run_id, status="completed")
        run = runlib.Run(run_id, os.path.join(runs_dir_path, run_id))
        gvar.index_register_run(run, root=runs_dir_path)

        # Drop cached conn AND remove the DB file so find_runs must go through
        # the slow path (which requires the lock). The run dir stays on disk.
        db_path = gvar._index_db_path(runs_dir_path)
        gvar._drop_cached_conn(f"conn_{db_path}")
        for suffix in ("", "-wal", "-shm", "-journal"):
            try:
                os.remove(db_path + suffix)
            except OSError:
                pass
        # Shrink the cached lock's timeout so the test doesn't wait 120s.
        lock_path = db_path + ".lock"
        gvar._index_locks[lock_path] = filelock.FileLock(lock_path, timeout=2)

        release, thread, _ = _hold_index_lock(runs_dir_path)
        try:
            t0 = time.time()
            results = list(gvar.find_runs(run_id, root=runs_dir_path))
            elapsed = time.time() - t0
        finally:
            release.set()
            thread.join(timeout=5)

        assert results, "find_runs returned no results under held lock"
        assert results[0][0] == run_id, f"unexpected run_id {results[0][0]}"
        assert elapsed < 10, f"find_runs took {elapsed:.1f}s; FS fallback should be fast"
        print(f"  find_runs returned run in {elapsed:.2f}s under held lock")
        print(f"  [PASS]")
        return True
    except AssertionError as e:
        print(f"  [FAIL] {e}")
        return False
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_write_raises_on_lock_timeout():
    """Failure B repro: _index_safe_write must re-raise filelock.Timeout
    so op finalization propagates a non-zero exit instead of silently
    ghosting the run."""
    print(f"\n=== Test: _index_safe_write raises on lock timeout ===")
    import filelock
    tmpdir = tempfile.mkdtemp(prefix="guild_test_")
    runs_dir_path = os.path.join(tmpdir, "runs")
    os.makedirs(runs_dir_path, exist_ok=True)
    os.environ["GUILD_HOME"] = tmpdir
    for mod in list(sys.modules):
        if mod.startswith("guild"):
            sys.modules.pop(mod, None)
    from guild import var as gvar
    from guild import run as runlib

    try:
        # Warm the DB so the cached lock entry exists.
        run_id = "b" * 32
        _make_run_dir(runs_dir_path, run_id, status="completed")
        run = runlib.Run(run_id, os.path.join(runs_dir_path, run_id))
        gvar.index_register_run(run, root=runs_dir_path)

        db_path = gvar._index_db_path(runs_dir_path)
        lock_path = db_path + ".lock"
        gvar._index_locks[lock_path] = filelock.FileLock(lock_path, timeout=2)

        release, thread, _ = _hold_index_lock(runs_dir_path)
        try:
            raised = False
            try:
                gvar._index_safe_write(lambda: None, root=runs_dir_path)
            except filelock.Timeout:
                raised = True
        finally:
            release.set()
            thread.join(timeout=5)

        assert raised, "_index_safe_write swallowed filelock.Timeout"
        print(f"  [PASS]")
        return True
    except AssertionError as e:
        print(f"  [FAIL] {e}")
        return False
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_worker_writes_are_noops_and_no_lock():
    """With GUILD_NO_INDEX_WRITES=1, every write path must be a no-op
    that only touches the dirty marker. Proved by holding the lock
    externally: if any write tried to acquire it, the call would block
    until the short timeout and raise filelock.Timeout."""
    print(f"\n=== Test: worker writes are no-ops and never touch the lock ===")
    import filelock
    tmpdir = tempfile.mkdtemp(prefix="guild_test_")
    runs_dir_path = os.path.join(tmpdir, "runs")
    os.makedirs(runs_dir_path, exist_ok=True)
    os.environ["GUILD_HOME"] = tmpdir
    for mod in list(sys.modules):
        if mod.startswith("guild"):
            sys.modules.pop(mod, None)
    from guild import var as gvar
    from guild import run as runlib

    try:
        # Seed the DB so the lock file exists and can be held.
        seed_id = "0" * 32
        _make_run_dir(runs_dir_path, seed_id, status="completed")
        seed = runlib.Run(seed_id, os.path.join(runs_dir_path, seed_id))
        gvar.index_register_run(seed, root=runs_dir_path)

        db_path = gvar._index_db_path(runs_dir_path)
        marker_path = db_path + ".dirty"
        db_mtime_before = os.path.getmtime(db_path)

        # Shrink the cached lock's timeout so if worker mode does leak a
        # lock acquisition, we fail in seconds instead of 120s.
        lock_path = db_path + ".lock"
        gvar._index_locks[lock_path] = filelock.FileLock(lock_path, timeout=2)

        os.environ[gvar._NO_INDEX_WRITES_ENV] = "1"

        release, thread, _ = _hold_index_lock(runs_dir_path)
        try:
            run_id = "w" * 32
            _make_run_dir(runs_dir_path, run_id, status="completed")
            run = runlib.Run(run_id, os.path.join(runs_dir_path, run_id))

            t0 = time.time()
            gvar.index_register_run(run, root=runs_dir_path)
            gvar.index_update_status(run, "pending", root=runs_dir_path)
            with gvar.index_batch_writes(root=runs_dir_path):
                gvar.index_update_status(run, "completed", root=runs_dir_path)
                gvar.index_update_attr(run, "started", int(time.time()),
                                       root=runs_dir_path)
            elapsed = time.time() - t0
        finally:
            release.set()
            thread.join(timeout=5)
            os.environ.pop(gvar._NO_INDEX_WRITES_ENV, None)

        assert elapsed < 1.0, (
            f"Worker writes took {elapsed:.2f}s; they must not wait on the lock"
        )
        assert os.path.exists(marker_path), "dirty marker not created"
        marker_mtime = os.path.getmtime(marker_path)
        # Clock resolution can make marker_mtime == db_mtime_before on the
        # same second; the important invariant is that the marker is not
        # older than the DB — a later non-worker op will see fresh-or-equal
        # and can still run the sync on the next strictly-fresher bump.
        assert marker_mtime >= db_mtime_before, (
            f"marker mtime ({marker_mtime}) older than db ({db_mtime_before})"
        )
        # DB unchanged: only the seeded run is there; the worker run is not.
        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute("SELECT run_id FROM runs").fetchall()
        finally:
            conn.close()
        run_ids = {r[0] for r in rows}
        assert run_id not in run_ids, (
            f"worker write leaked into DB: {run_ids}"
        )
        assert seed_id in run_ids, "seeded run missing from DB"

        print(f"  worker writes: {elapsed*1000:.1f}ms under held lock")
        print(f"  [PASS]")
        return True
    except AssertionError as e:
        print(f"  [FAIL] {e}")
        return False
    finally:
        os.environ.pop("GUILD_NO_INDEX_WRITES", None)
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_next_op_syncs_on_dirty():
    """After a worker touches the dirty marker, the next non-worker
    _get_index_conn must run a full sync and then clear the marker."""
    print(f"\n=== Test: next op syncs on dirty marker ===")
    tmpdir = tempfile.mkdtemp(prefix="guild_test_")
    runs_dir_path = os.path.join(tmpdir, "runs")
    os.makedirs(runs_dir_path, exist_ok=True)
    os.environ["GUILD_HOME"] = tmpdir
    for mod in list(sys.modules):
        if mod.startswith("guild"):
            sys.modules.pop(mod, None)
    from guild import var as gvar
    from guild import run as runlib

    try:
        # Seed with run A via the normal write path.
        a_id = "a" * 32
        _make_run_dir(runs_dir_path, a_id, status="completed")
        a = runlib.Run(a_id, os.path.join(runs_dir_path, a_id))
        gvar.index_register_run(a, root=runs_dir_path)
        gvar.index_update_status(a, "completed", root=runs_dir_path)

        # Create run B on disk but NOT in the index (simulates a worker run).
        b_id = "b" * 32
        _make_run_dir(runs_dir_path, b_id, status="completed")

        db_path = gvar._index_db_path(runs_dir_path)
        marker_path = db_path + ".dirty"

        # Touch the marker with an mtime strictly greater than the DB.
        with open(marker_path, "a"):
            pass
        future = os.path.getmtime(db_path) + 5
        os.utime(marker_path, (future, future))

        # Drop cached conn to simulate a fresh CLI process.
        gvar._drop_cached_conn(f"conn_{db_path}")
        gvar._remove_sync_marker(runs_dir_path)

        results = list(gvar.find_runs("", root=runs_dir_path))
        ids = {rid for rid, _ in results}

        assert b_id in ids, f"dirty sync did not pick up B: {ids}"
        assert a_id in ids, f"dirty sync dropped A: {ids}"
        assert not os.path.exists(marker_path), (
            "dirty marker should be cleared after successful sync"
        )
        print(f"  [PASS]")
        return True
    except AssertionError as e:
        print(f"  [FAIL] {e}")
        return False
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_racing_touch_preserves_marker():
    """If a worker touches the marker during a running sync, the
    compare-and-delete must leave the marker in place so the next op
    re-syncs."""
    print(f"\n=== Test: racing touch preserves dirty marker ===")
    tmpdir = tempfile.mkdtemp(prefix="guild_test_")
    runs_dir_path = os.path.join(tmpdir, "runs")
    os.makedirs(runs_dir_path, exist_ok=True)
    os.environ["GUILD_HOME"] = tmpdir
    for mod in list(sys.modules):
        if mod.startswith("guild"):
            sys.modules.pop(mod, None)
    from guild import var as gvar
    from guild import run as runlib

    try:
        seed_id = "c" * 32
        _make_run_dir(runs_dir_path, seed_id, status="completed")
        seed = runlib.Run(seed_id, os.path.join(runs_dir_path, seed_id))
        gvar.index_register_run(seed, root=runs_dir_path)

        db_path = gvar._index_db_path(runs_dir_path)
        marker_path = db_path + ".dirty"

        # Make the marker fresher than the DB.
        with open(marker_path, "a"):
            pass
        future = os.path.getmtime(db_path) + 5
        os.utime(marker_path, (future, future))

        # Monkey-patch _do_index_sync to race: bump the marker mid-sync.
        original = gvar._do_index_sync
        def racing_sync(conn, root, add_only=False):
            racing_sync.called += 1
            try:
                original(conn, root, add_only=add_only)
            finally:
                future2 = os.path.getmtime(marker_path) + 10
                os.utime(marker_path, (future2, future2))
        racing_sync.called = 0
        gvar._do_index_sync = racing_sync

        try:
            gvar._drop_cached_conn(f"conn_{db_path}")
            gvar._remove_sync_marker(runs_dir_path)
            list(gvar.find_runs("", root=runs_dir_path))
        finally:
            gvar._do_index_sync = original

        assert racing_sync.called == 1, (
            f"sync should have run exactly once, ran {racing_sync.called}"
        )
        assert os.path.exists(marker_path), (
            "racing touch should have prevented marker cleanup"
        )
        print(f"  [PASS]")
        return True
    except AssertionError as e:
        print(f"  [FAIL] {e}")
        return False
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_worker_reads_never_trigger_sync():
    """With GUILD_NO_INDEX_WRITES=1, reads must never invoke _do_index_sync
    even when the dirty marker is fresh, to avoid thundering-herd syncs
    and any worker acquiring the write lock."""
    print(f"\n=== Test: worker reads never trigger sync ===")
    tmpdir = tempfile.mkdtemp(prefix="guild_test_")
    runs_dir_path = os.path.join(tmpdir, "runs")
    os.makedirs(runs_dir_path, exist_ok=True)
    os.environ["GUILD_HOME"] = tmpdir
    for mod in list(sys.modules):
        if mod.startswith("guild"):
            sys.modules.pop(mod, None)
    from guild import var as gvar
    from guild import run as runlib

    try:
        run_id = "d" * 32
        _make_run_dir(runs_dir_path, run_id, status="completed")
        run = runlib.Run(run_id, os.path.join(runs_dir_path, run_id))
        gvar.index_register_run(run, root=runs_dir_path)

        db_path = gvar._index_db_path(runs_dir_path)
        marker_path = db_path + ".dirty"

        with open(marker_path, "a"):
            pass
        future = os.path.getmtime(db_path) + 5
        os.utime(marker_path, (future, future))

        sync_calls = []
        original = gvar._do_index_sync
        def counting_sync(*a, **kw):
            sync_calls.append(1)
            return original(*a, **kw)
        gvar._do_index_sync = counting_sync

        os.environ[gvar._NO_INDEX_WRITES_ENV] = "1"
        try:
            gvar._drop_cached_conn(f"conn_{db_path}")
            list(gvar.find_runs(run_id, root=runs_dir_path))
        finally:
            gvar._do_index_sync = original
            os.environ.pop(gvar._NO_INDEX_WRITES_ENV, None)

        assert not sync_calls, (
            f"worker read invoked sync {len(sync_calls)} times; must be 0"
        )
        assert os.path.exists(marker_path), "worker must not clear marker"
        print(f"  [PASS]")
        return True
    except AssertionError as e:
        print(f"  [FAIL] {e}")
        return False
    finally:
        os.environ.pop("GUILD_NO_INDEX_WRITES", None)
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_dirty_sync_refreshes_existing_row_status():
    """Mixed headnode+worker flow: headnode registers a run as pending,
    worker finalizes it on disk (no index write, just dirty marker). The
    next headnode op must refresh the indexed status from disk, not leave
    it stuck at 'pending'."""
    print(f"\n=== Test: dirty sync refreshes stale status on existing rows ===")
    tmpdir = tempfile.mkdtemp(prefix="guild_test_")
    runs_dir_path = os.path.join(tmpdir, "runs")
    os.makedirs(runs_dir_path, exist_ok=True)
    os.environ["GUILD_HOME"] = tmpdir
    for mod in list(sys.modules):
        if mod.startswith("guild"):
            sys.modules.pop(mod, None)
    from guild import var as gvar
    from guild import run as runlib

    try:
        # Headnode: register run as pending, on-disk it has no exit_status yet.
        run_id = "e" * 32
        run_path = os.path.join(runs_dir_path, run_id)
        guild_dir = os.path.join(run_path, ".guild")
        attrs_dir = os.path.join(guild_dir, "attrs")
        os.makedirs(attrs_dir, exist_ok=True)
        with open(os.path.join(guild_dir, "opref"), "w") as f:
            f.write("test:test")
        ts = str(int(time.time() * 1000000))
        with open(os.path.join(attrs_dir, "initialized"), "w") as f:
            f.write(ts)
        run = runlib.Run(run_id, run_path)
        gvar.index_register_run(run, root=runs_dir_path)
        gvar.index_update_status(run, "pending", root=runs_dir_path)

        db_path = gvar._index_db_path(runs_dir_path)

        # Confirm DB has pending status.
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        finally:
            conn.close()
        assert row and row[0] == "pending", (
            f"seed status should be pending, got {row}"
        )

        # Worker: write exit_status + started on disk, touch dirty marker,
        # never touch the index.
        with open(os.path.join(attrs_dir, "started"), "w") as f:
            f.write(ts)
        with open(os.path.join(attrs_dir, "exit_status"), "w") as f:
            f.write("0")
        marker_path = db_path + ".dirty"
        with open(marker_path, "a"):
            pass
        future = os.path.getmtime(db_path) + 5
        os.utime(marker_path, (future, future))

        # Headnode: fresh CLI process. Drop cached conn so dirty path fires.
        gvar._drop_cached_conn(f"conn_{db_path}")

        # Triggering a read must run the dirty sync and update the status.
        list(gvar.find_runs("", root=runs_dir_path))

        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        finally:
            conn.close()

        assert row and row[0] == "completed", (
            f"dirty sync did not refresh stale status: row={row}"
        )
        assert not os.path.exists(marker_path), (
            "dirty marker should be cleared after successful sync"
        )
        print(f"  [PASS]")
        return True
    except AssertionError as e:
        print(f"  [FAIL] {e}")
        return False
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_dirty_flag_during_run_updates_on_finalize():
    """The dirty flag may be set while a worker run is still in-flight
    (started on disk, no exit_status yet, PID on a different NFS host).
    A sync triggered mid-flight must NOT falsely mark the run as 'error'.
    Once the worker finishes and sets the flag again, the next sync must
    correctly update the status to its terminal value."""
    print(f"\n=== Test: dirty flag set during run updates correctly on finalize ===")
    tmpdir = tempfile.mkdtemp(prefix="guild_test_")
    runs_dir_path = os.path.join(tmpdir, "runs")
    os.makedirs(runs_dir_path, exist_ok=True)
    os.environ["GUILD_HOME"] = tmpdir
    for mod in list(sys.modules):
        if mod.startswith("guild"):
            sys.modules.pop(mod, None)
    from guild import var as gvar
    from guild import run as runlib

    try:
        # Headnode registers run A as pending; on disk it's only initialized.
        run_id = "f" * 32
        run_path = os.path.join(runs_dir_path, run_id)
        guild_dir = os.path.join(run_path, ".guild")
        attrs_dir = os.path.join(guild_dir, "attrs")
        os.makedirs(attrs_dir, exist_ok=True)
        with open(os.path.join(guild_dir, "opref"), "w") as f:
            f.write("test:test")
        ts = str(int(time.time() * 1000000))
        with open(os.path.join(attrs_dir, "initialized"), "w") as f:
            f.write(ts)
        run = runlib.Run(run_id, run_path)
        gvar.index_register_run(run, root=runs_dir_path)
        gvar.index_update_status(run, "pending", root=runs_dir_path)

        db_path = gvar._index_db_path(runs_dir_path)
        marker_path = db_path + ".dirty"

        def read_status():
            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    "SELECT status FROM runs WHERE run_id = ?", (run_id,)
                ).fetchone()
            finally:
                conn.close()
            return row[0] if row else None

        # Mid-run: worker has written `started`, dirty-touched, but no
        # exit_status yet (process still executing on another host).
        with open(os.path.join(attrs_dir, "started"), "w") as f:
            f.write(ts)
        with open(marker_path, "a"):
            pass
        future = os.path.getmtime(db_path) + 5
        os.utime(marker_path, (future, future))

        gvar._drop_cached_conn(f"conn_{db_path}")
        list(gvar.find_runs("", root=runs_dir_path))

        mid_status = read_status()
        assert mid_status == "pending", (
            f"mid-run sync must not overwrite stale status with false 'error';"
            f" got {mid_status!r}"
        )

        # The marker got cleared because the sync ran; the in-flight row
        # was skipped (not upserted). A later touch will re-trigger sync.

        # Run finalizes: worker writes exit_status=0, touches marker again.
        with open(os.path.join(attrs_dir, "exit_status"), "w") as f:
            f.write("0")
        with open(marker_path, "a"):
            pass
        future = os.path.getmtime(db_path) + 5
        os.utime(marker_path, (future, future))

        gvar._drop_cached_conn(f"conn_{db_path}")
        list(gvar.find_runs("", root=runs_dir_path))

        final_status = read_status()
        assert final_status == "completed", (
            f"post-finalize sync must upsert terminal status; got {final_status!r}"
        )
        assert not os.path.exists(marker_path), (
            "dirty marker should be cleared after successful sync"
        )

        # Also verify a brand-new worker run (never registered on headnode)
        # with the same lifecycle: in-flight sync skips, finalize-sync adds.
        new_id = "9" * 32
        new_path = os.path.join(runs_dir_path, new_id)
        new_guild = os.path.join(new_path, ".guild")
        new_attrs = os.path.join(new_guild, "attrs")
        os.makedirs(new_attrs, exist_ok=True)
        with open(os.path.join(new_guild, "opref"), "w") as f:
            f.write("test:test")
        with open(os.path.join(new_attrs, "initialized"), "w") as f:
            f.write(ts)
        with open(os.path.join(new_attrs, "started"), "w") as f:
            f.write(ts)
        with open(marker_path, "a"):
            pass
        future = os.path.getmtime(db_path) + 5
        os.utime(marker_path, (future, future))

        gvar._drop_cached_conn(f"conn_{db_path}")
        list(gvar.find_runs("", root=runs_dir_path))

        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                "SELECT status FROM runs WHERE run_id = ?", (new_id,)
            ).fetchone()
        finally:
            conn.close()
        assert row is None, (
            f"in-flight new run must not be inserted with false status; got {row}"
        )

        # Finalize the new run.
        with open(os.path.join(new_attrs, "exit_status"), "w") as f:
            f.write("0")
        with open(marker_path, "a"):
            pass
        future = os.path.getmtime(db_path) + 5
        os.utime(marker_path, (future, future))

        gvar._drop_cached_conn(f"conn_{db_path}")
        list(gvar.find_runs("", root=runs_dir_path))

        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                "SELECT status FROM runs WHERE run_id = ?", (new_id,)
            ).fetchone()
        finally:
            conn.close()
        assert row and row[0] == "completed", (
            f"finalized new run should be indexed as completed; got {row}"
        )

        print(f"  [PASS]")
        return True
    except AssertionError as e:
        print(f"  [FAIL] {e}")
        return False
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    results = {}
    results["concurrent_writes"] = test_concurrent_writes()
    results["concurrent_read_write"] = test_concurrent_read_write()
    results["flag_filter"] = test_flag_filter_concurrent()
    results["status_filter"] = test_status_filter_concurrent()
    results["transient_error_no_nuke"] = test_transient_error_no_nuke()
    results["filter_speed_10k"] = test_filter_speed_10k()
    results["find_runs_fs_fallback_under_lock"] = test_find_runs_fs_fallback_under_lock()
    results["write_raises_on_lock_timeout"] = test_write_raises_on_lock_timeout()
    results["worker_writes_are_noops"] = test_worker_writes_are_noops_and_no_lock()
    results["next_op_syncs_on_dirty"] = test_next_op_syncs_on_dirty()
    results["racing_touch_preserves_marker"] = test_racing_touch_preserves_marker()
    results["worker_reads_never_trigger_sync"] = test_worker_reads_never_trigger_sync()
    results["dirty_sync_refreshes_existing_row_status"] = test_dirty_sync_refreshes_existing_row_status()
    results["dirty_flag_during_run_updates_on_finalize"] = test_dirty_flag_during_run_updates_on_finalize()

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
