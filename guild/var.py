# Copyright 2017-2023 Posit Software, PBC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import errno
import functools
import contextlib
import json
import logging
import os
import shutil
import sqlite3
import tempfile
import threading
import time

import filelock

from guild import config
from guild import run as runlib
from guild import util

log = logging.getLogger("guild")

_index_local = threading.local()


def _index_db_path(root=None):
    import hashlib
    root = root or runs_dir()
    env_dir = os.environ.get("GUILD_DATABASE")
    if env_dir:
        os.makedirs(env_dir, exist_ok=True)
        root_hash = hashlib.md5(root.encode()).hexdigest()[:16]
        return os.path.join(env_dir, f"index_{root_hash}.db")
    return os.path.join(root, ".guild_index.db")


_index_locks = {}
_index_locks_mu = threading.Lock()


def _get_index_lock(root=None):
    db_path = _index_db_path(root)
    lock_path = db_path + ".lock"
    with _index_locks_mu:
        lock = _index_locks.get(lock_path)
        if lock is None:
            lock = filelock.FileLock(lock_path, timeout=120)
            _index_locks[lock_path] = lock
        return lock


_CORRUPTION_SIGNALS = (
    "database disk image is malformed",
    "file is not a database",
    "file is encrypted or is not a database",
    "database is corrupt",
    "malformed database schema",
)


def _is_corruption_error(exc):
    msg = str(exc).lower()
    return any(sig in msg for sig in _CORRUPTION_SIGNALS)


def _sync_marker_path(root=None):
    return _index_db_path(root) + ".synced"


def _write_sync_marker(root=None):
    try:
        with open(_sync_marker_path(root), "w") as f:
            f.write(str(int(time.time())))
    except OSError as e:
        log.debug("Could not write sync marker: %s", e)


def _remove_sync_marker(root=None):
    try:
        os.remove(_sync_marker_path(root))
    except OSError:
        pass


def _sync_marker_exists(root=None):
    return os.path.exists(_sync_marker_path(root))


_NO_INDEX_WRITES_ENV = "GUILD_NO_INDEX_WRITES"


def _writes_disabled():
    return os.environ.get(_NO_INDEX_WRITES_ENV, "") not in ("", "0", "false", "False")


def _dirty_marker_path(root=None):
    return _index_db_path(root) + ".dirty"


def _touch_dirty_marker(root=None):
    path = _dirty_marker_path(root)
    try:
        with open(path, "a"):
            pass
        os.utime(path, None)
    except OSError as e:
        log.debug("Failed to touch index dirty marker at %s: %s", path, e)


def _dirty_marker_mtime(root=None):
    try:
        return os.path.getmtime(_dirty_marker_path(root))
    except OSError:
        return None


def _dirty_marker_fresher_than_db(root=None):
    marker_mtime = _dirty_marker_mtime(root)
    if marker_mtime is None:
        return False
    try:
        db_mtime = os.path.getmtime(_index_db_path(root))
    except OSError:
        return True
    return marker_mtime > db_mtime


def _clear_dirty_marker_if_unchanged(root, seen_mtime):
    path = _dirty_marker_path(root)
    try:
        if os.path.getmtime(path) == seen_mtime:
            os.remove(path)
    except OSError:
        pass


def _dirty_runs_dir(root=None):
    return _index_db_path(root) + ".dirty.d"


def _touch_run_dirty_marker(root, run_id):
    """Mark a single run as needing resync. Workers call this per write
    instead of touching the global dirty marker, so the headnode can do a
    delta sync over only the touched runs.
    """
    d = _dirty_runs_dir(root)
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        return
    path = os.path.join(d, run_id)
    try:
        with open(path, "a"):
            pass
        os.utime(path, None)
    except OSError as e:
        log.debug("Failed to touch per-run dirty marker at %s: %s", path, e)


def _list_dirty_run_markers(root=None):
    """Return [(run_id, mtime)] for per-run dirty markers, or [] if none."""
    d = _dirty_runs_dir(root)
    out = []
    try:
        names = os.listdir(d)
    except OSError:
        return out
    for name in names:
        if len(name) != 32:
            continue
        try:
            mtime = os.path.getmtime(os.path.join(d, name))
        except OSError:
            continue
        out.append((name, mtime))
    return out


def _clear_run_dirty_marker_if_unchanged(root, run_id, seen_mtime):
    path = os.path.join(_dirty_runs_dir(root), run_id)
    try:
        if os.path.getmtime(path) == seen_mtime:
            os.remove(path)
    except OSError:
        pass


def _clear_run_dirty_markers_up_to(root, cutoff_mtime):
    """Remove per-run markers whose mtime is <= cutoff_mtime.

    Called after a full resync, which already covers every on-disk run.
    Markers created after `cutoff_mtime` are left alone: they represent
    worker writes that raced with the sync and should trigger a delta
    sync on the next read.
    """
    d = _dirty_runs_dir(root)
    try:
        names = os.listdir(d)
    except OSError:
        return
    for name in names:
        path = os.path.join(d, name)
        try:
            if os.path.getmtime(path) <= cutoff_mtime:
                os.remove(path)
        except OSError:
            pass


def _nuke_index(root=None):
    """Delete the index DB. MUST be called with _get_index_lock held."""
    root = root or runs_dir()
    db_path = _index_db_path(root)
    cache_key = f"conn_{db_path}"
    # Reentrantly acquire the lock; callers already holding it pay nothing.
    with _get_index_lock(root):
        conn = getattr(_index_local, cache_key, None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            try:
                delattr(_index_local, cache_key)
            except AttributeError:
                pass
        for suffix in ("", "-wal", "-shm", "-journal"):
            try:
                os.remove(db_path + suffix)
            except OSError:
                pass
        _remove_sync_marker(root)


def _init_index_schema(conn):
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS runs ("
        "  run_id TEXT PRIMARY KEY,"
        "  status TEXT,"
        "  opref TEXT,"
        "  op_name TEXT,"
        "  started INTEGER,"
        "  initialized INTEGER,"
        "  label TEXT,"
        "  flags TEXT,"
        "  tags TEXT"
        ")"
    )
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(runs)").fetchall()}
    for col in ("label", "flags", "tags", "op_name"):
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE runs ADD COLUMN {col} TEXT")
    conn.commit()


def _connect_db(db_path):
    conn = sqlite3.connect(db_path, timeout=300)
    conn.execute("PRAGMA busy_timeout=300000")
    return conn


def _drop_cached_conn(cache_key):
    conn = getattr(_index_local, cache_key, None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
        try:
            delattr(_index_local, cache_key)
        except AttributeError:
            pass


def _should_sync(conn, root):
    """Whether to run a full filesystem sync on this fresh connection.

    Skip sync when the marker file exists (another process synced recently).
    Otherwise sync only if the DB is empty but the runs directory has candidates.
    If the DB already has data, trust it and don't re-sync.
    """
    if _sync_marker_exists(root):
        return False
    try:
        count = conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
    except sqlite3.OperationalError:
        return False
    if count == 0:
        try:
            for name in os.listdir(root):
                if len(name) == 32:
                    return True
        except OSError:
            pass
    return False


def _get_index_conn(root=None):
    root = root or runs_dir()
    db_path = _index_db_path(root)
    cache_key = f"conn_{db_path}"

    # Dirty-triggered resync. Workers skip this: they read whatever
    # their view of the shared index shows and rely on the FS fallback in
    # find_runs for cache misses, so they never touch the write lock.
    # The in_dirty_sync guard prevents recursive re-sync: _do_index_sync
    # constructs Run objects whose property access re-enters this function,
    # and those nested calls must return the cached conn, not start another
    # sync that would drop the conn we're writing through.
    #
    # Two granularities are supported:
    #   - global marker (<db>.dirty) fresh → full resync of every on-disk run
    #   - per-run markers (<db>.dirty.d/<run_id>) fresh → delta sync of just
    #     the listed runs (the common case, set by worker writes)
    dirty_mtime = None
    per_run_markers = None
    if (not _writes_disabled()
            and not getattr(_index_local, 'in_dirty_sync', False)):
        dirty_mtime = _dirty_marker_mtime(root)
        if dirty_mtime is not None and _dirty_marker_fresher_than_db(root):
            _drop_cached_conn(cache_key)
        else:
            dirty_mtime = None
        if dirty_mtime is None:
            markers = _list_dirty_run_markers(root)
            if markers:
                try:
                    db_mtime = os.path.getmtime(db_path)
                except OSError:
                    db_mtime = 0
                fresh = [(r, m) for (r, m) in markers if m > db_mtime]
                if fresh:
                    per_run_markers = fresh
                    _drop_cached_conn(cache_key)

    if dirty_mtime is None and per_run_markers is None:
        conn = getattr(_index_local, cache_key, None)
        if conn is not None:
            try:
                conn.execute("SELECT 1 FROM runs LIMIT 1")
                return conn
            except sqlite3.OperationalError:
                _drop_cached_conn(cache_key)
            except sqlite3.DatabaseError:
                _drop_cached_conn(cache_key)

        # Fast path: DB file already exists and is healthy. Open and probe
        # without taking the filelock. Most concurrent `guild` invocations
        # land here and never serialize on the lock.
        if os.path.exists(db_path):
            fast_conn = None
            try:
                fast_conn = _connect_db(db_path)
                fast_conn.execute("SELECT 1 FROM runs LIMIT 1")
                setattr(_index_local, cache_key, fast_conn)
                return fast_conn
            except sqlite3.OperationalError:
                if fast_conn is not None:
                    try:
                        fast_conn.close()
                    except Exception:
                        pass
            except sqlite3.DatabaseError:
                if fast_conn is not None:
                    try:
                        fast_conn.close()
                    except Exception:
                        pass

    # Slow path: take the lock for init / migration / repair.
    # Sync happens OUTSIDE the lock to avoid blocking writers during a
    # multi-second filesystem scan.
    conn = None
    sync_kind = None  # None | 'add_only' | 'full'
    try:
        with _get_index_lock(root):
            conn = None
            try:
                conn = _connect_db(db_path)
                _init_index_schema(conn)
                conn.execute("SELECT 1 FROM runs LIMIT 1")
            except sqlite3.OperationalError:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                conn = _connect_db(db_path)
                _init_index_schema(conn)
            except sqlite3.DatabaseError as e:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                if _is_corruption_error(e):
                    log.warning(
                        "Index DB corruption confirmed (%s) - rebuilding", e
                    )
                    _nuke_index(root)
                    conn = _connect_db(db_path)
                    _init_index_schema(conn)
                else:
                    raise
            setattr(_index_local, cache_key, conn)
            if dirty_mtime is not None:
                sync_kind = 'full'
            elif per_run_markers is not None:
                sync_kind = 'delta'
            elif _should_sync(conn, root):
                _write_sync_marker(root)
                sync_kind = 'add_only'
    except filelock.Timeout:
        log.warning(
            "Timeout acquiring index lock; caller will fall back to FS scan"
        )
        raise sqlite3.OperationalError("timeout acquiring index lock")

    # Sync OUTSIDE the lock: this can take several seconds for large runs dirs.
    if sync_kind == 'full':
        _index_local.in_dirty_sync = True
        sync_start = time.time()
        try:
            _do_index_sync(conn, root, add_only=False)
            _clear_dirty_marker_if_unchanged(root, dirty_mtime)
            _clear_run_dirty_markers_up_to(root, sync_start)
        except Exception as e:
            log.warning("Dirty-triggered index sync failed: %s", e)
        finally:
            _index_local.in_dirty_sync = False
    elif sync_kind == 'delta':
        _index_local.in_dirty_sync = True
        try:
            _do_delta_sync(conn, root, per_run_markers)
        except Exception as e:
            log.warning("Per-run delta sync failed: %s", e)
        finally:
            _index_local.in_dirty_sync = False
    elif sync_kind == 'add_only':
        try:
            _do_index_sync(conn, root, add_only=True)
        except Exception as e:
            log.warning("Index auto-sync failed, will retry next cold start: %s", e)
            _remove_sync_marker(root)

    return conn


def _index_safe_write(fn, root=None, run_id=None):
    """Run fn under the index lock, retrying once on transient errors.

    Only rebuilds the index when SQLite reports actual file-level
    corruption, not on any DatabaseError. This avoids nuke-storms where a
    single transient failure empties the DB and forces every subsequent
    process through a full re-sync.

    In worker (writes-disabled) mode, fn is skipped and a dirty marker is
    touched instead. When run_id is given, the per-run marker is touched so
    the next headnode read can delta-sync only that run; otherwise the
    global marker is touched, forcing a full resync.
    """
    if _writes_disabled():
        if run_id is not None:
            _touch_run_dirty_marker(root, run_id)
        else:
            _touch_dirty_marker(root)
        return
    try:
        with _get_index_lock(root):
            try:
                fn()
                return
            except sqlite3.OperationalError as e:
                log.debug("Index write transient error, retrying: %s", e)
            except sqlite3.DatabaseError as e:
                if _is_corruption_error(e):
                    log.warning(
                        "Index DB corruption on write (%s) - rebuilding", e
                    )
                    _nuke_index(root)
                    return
                log.debug("Index write retryable DatabaseError: %s", e)
            # Drop any cached conn so the retry reopens fresh.
            _drop_cached_conn(f"conn_{_index_db_path(root)}")
            try:
                fn()
            except sqlite3.DatabaseError as e:
                if _is_corruption_error(e):
                    log.warning(
                        "Index DB corruption confirmed on retry (%s) - "
                        "rebuilding", e
                    )
                    _nuke_index(root)
                else:
                    log.warning(
                        "Index write failed after retry, leaving DB intact: %s",
                        e,
                    )
    except filelock.Timeout:
        log.error("Timeout acquiring index lock for write after 120s")
        raise


@contextlib.contextmanager
def index_batch_writes(root=None):
    if _writes_disabled():
        # Per-run markers are touched by the individual writers inside the
        # batch (pending_writes is not set in worker mode, so each call
        # falls through to _index_safe_write with its run_id). No global
        # marker touch here — we don't want to force a full resync for a
        # batch whose writes are already tracked at per-run granularity.
        yield
        return
    depth = getattr(_index_local, 'batch_depth', 0)
    if depth == 0:
        _index_local.pending_writes = {}
    _index_local.batch_depth = depth + 1
    try:
        yield
    finally:
        _index_local.batch_depth -= 1
        if _index_local.batch_depth == 0:
            _flush_pending_writes(root)
            _index_local.pending_writes = None


def _flush_pending_writes(root=None):
    if _writes_disabled():
        # Defensive path: index_batch_writes in worker mode doesn't install
        # pending_writes, so writers inside the batch already touched their
        # per-run markers. If some code path somehow left a pending dict,
        # mirror its keys into per-run markers rather than forcing a global
        # resync.
        pending = getattr(_index_local, 'pending_writes', None)
        if pending:
            for rid in pending:
                _touch_run_dirty_marker(root, rid)
        return
    pending = getattr(_index_local, 'pending_writes', None)
    if not pending:
        return
    def _do():
        conn = _get_index_conn(root)
        for run_id, changes in pending.items():
            if changes.get('_delete'):
                conn.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))
                continue
            if changes.get('_register'):
                _index_upsert_run(conn, changes['_run'])
            updates = {k: v for k, v in changes.items() if not k.startswith('_')}
            if updates:
                set_clause = ", ".join(f"{col} = ?" for col in updates)
                conn.execute(
                    f"UPDATE runs SET {set_clause} WHERE run_id = ?",
                    list(updates.values()) + [run_id],
                )
        conn.commit()
    try:
        _index_safe_write(_do, root)
    except Exception:
        if os.getenv("GUILD_OP_TRACE") == "1":
            import sys, traceback
            sys.stderr.write("[guild-op-trace] flush: raised\n")
            traceback.print_exc()
        raise


_SYNC_UPSERT_BATCH = 200


def _has_definitive_status(run_path):
    """True if the on-disk run has unambiguous status without a PID check.

    Without one of these signals, Run._local_status falls back to checking
    whether the run's PID is alive on the *local* host — which is wrong for
    runs executing on a different NFS node (they look dead and get marked
    "error"). Dirty-triggered full resyncs use this to skip in-flight runs
    and wait for the next sync after they write exit_status.
    """
    guild_dir = os.path.join(run_path, ".guild")
    if os.path.exists(os.path.join(guild_dir, "attrs", "exit_status")):
        return True
    for marker in ("STAGED", "PENDING", "LOCK.remote"):
        if os.path.exists(os.path.join(guild_dir, marker)):
            return True
    return False


def _do_index_sync(conn, root, add_only=False):
    """Synchronize index with filesystem.

    When add_only=True (auto-sync from _get_index_conn), only add new runs.
    Never delete existing entries (in-flight runs may have been registered but
    not yet have opref written). Stale entries are pruned lazily by query paths.

    When add_only=False (explicit index_sync), do full reconciliation including
    deletion of truly stale entries.
    """
    indexed = {row[0] for row in conn.execute("SELECT run_id FROM runs").fetchall()}
    on_disk = set()
    try:
        names = os.listdir(root)
    except OSError:
        names = []
    new_runs = []
    existing_runs = []
    for name in names:
        if len(name) != 32:
            continue
        path = os.path.join(root, name)
        if not _opref_exists(path):
            continue
        on_disk.add(name)
        if name in indexed:
            existing_runs.append((name, path))
        else:
            new_runs.append((name, path))
    # On a full sync (add_only=False) we must also refresh existing rows:
    # workers in GUILD_NO_INDEX_WRITES mode finalize runs on disk without
    # updating the index, so the dirty-triggered sync is the only place
    # their terminal status can land. Skip runs without definitive on-disk
    # state (in-flight runs on another host) — a future sync will pick them
    # up once they have exit_status.
    if add_only:
        to_upsert = new_runs
    else:
        to_upsert = [
            (n, p) for (n, p) in new_runs + existing_runs
            if _has_definitive_status(p)
        ]
    # Commit in batches so a crash mid-sync doesn't lose all progress and
    # so other writers aren't blocked by one mega-transaction.
    for i in range(0, len(to_upsert), _SYNC_UPSERT_BATCH):
        for name, path in to_upsert[i:i + _SYNC_UPSERT_BATCH]:
            run = runlib.Run(name, path)
            _index_upsert_run(conn, run)
        conn.commit()
    # Only delete stale entries when doing full sync, not during auto-sync.
    if not add_only:
        stale = indexed - on_disk
        if stale:
            stale_list = [(rid,) for rid in stale]
            for i in range(0, len(stale_list), _SYNC_UPSERT_BATCH):
                conn.executemany(
                    "DELETE FROM runs WHERE run_id = ?",
                    stale_list[i:i + _SYNC_UPSERT_BATCH],
                )
                conn.commit()
    _write_sync_marker(root)


def _do_delta_sync(conn, root, markers):
    """Upsert only the runs listed in `markers`, then clear each marker.

    markers: list of (run_id, mtime) tuples from _list_dirty_run_markers.

    Runs without an opref on disk are treated as removed. In-flight runs
    (no definitive status) are skipped and their markers are left in place,
    so the next sync retries after the worker writes exit_status.

    Marker clearing is compare-by-mtime: if a worker re-touches the marker
    while we're processing, it survives the clear and the next read will
    pick it up.
    """
    for run_id, mtime in markers:
        path = os.path.join(root, run_id)
        if not os.path.isdir(path) or not _opref_exists(path):
            conn.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))
            _clear_run_dirty_marker_if_unchanged(root, run_id, mtime)
            continue
        if not _has_definitive_status(path):
            continue
        run = runlib.Run(run_id, path)
        _index_upsert_run(conn, run)
        _clear_run_dirty_marker_if_unchanged(root, run_id, mtime)
    conn.commit()


def index_sync(root=None):
    root = root or runs_dir()
    _remove_sync_marker(root)
    conn = _get_index_conn(root)
    _index_safe_write(lambda: _do_index_sync(conn, root, add_only=False), root)


def _index_upsert_run(conn, run):
    from guild import run_util

    opref_str = ""
    try:
        opref_str = str(run.opref) if run.opref else ""
    except Exception:
        pass
    op_name = ""
    try:
        op_name = run_util.format_operation(run, nowarn=True)
    except Exception:
        pass
    status = run.status
    started = run.get("started")
    initialized = run.get("initialized")
    label = run.get("label", "")
    flags = ""
    try:
        f = run.get("flags")
        if f:
            flags = json.dumps(f)
    except Exception:
        pass
    tags = ""
    try:
        t = run.get("tags")
        if t:
            tags = json.dumps(t)
    except Exception:
        pass
    conn.execute(
        "INSERT OR REPLACE INTO runs "
        "(run_id, status, opref, op_name, started, initialized, label, flags, tags) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (run.id, status, opref_str, op_name, started, initialized, label, flags, tags),
    )


def index_update_status(run, status, root=None):
    pending = getattr(_index_local, 'pending_writes', None)
    if pending is not None:
        pending.setdefault(run.id, {})['status'] = status
        return
    def _do():
        conn = _get_index_conn(root)
        conn.execute(
            "UPDATE runs SET status = ? WHERE run_id = ?",
            (status, run.id),
        )
        if conn.total_changes == 0:
            _index_upsert_run(conn, run)
        conn.commit()
    _index_safe_write(_do, root, run_id=run.id)


def index_update_attr(run, name, val, root=None):
    col_map = {
        "label": "label",
        "started": "started",
        "initialized": "initialized",
    }
    pending = getattr(_index_local, 'pending_writes', None)
    if pending is not None:
        entry = pending.setdefault(run.id, {})
        if name == "flags":
            entry['flags'] = json.dumps(val) if val else ""
        elif name == "tags":
            entry['tags'] = json.dumps(val) if val else ""
        elif name in col_map:
            entry[col_map[name]] = val
        return
    def _do():
        conn = _get_index_conn(root)
        if name == "flags":
            db_val = json.dumps(val) if val else ""
            conn.execute(
                "UPDATE runs SET flags = ? WHERE run_id = ?",
                (db_val, run.id),
            )
        elif name == "tags":
            db_val = json.dumps(val) if val else ""
            conn.execute(
                "UPDATE runs SET tags = ? WHERE run_id = ?",
                (db_val, run.id),
            )
        elif name in col_map:
            conn.execute(
                f"UPDATE runs SET {col_map[name]} = ? WHERE run_id = ?",
                (val, run.id),
            )
        else:
            return
        conn.commit()
    _index_safe_write(_do, root, run_id=run.id)


def index_register_run(run, root=None):
    pending = getattr(_index_local, 'pending_writes', None)
    if pending is not None:
        entry = pending.setdefault(run.id, {})
        entry['_register'] = True
        entry['_run'] = run
        return
    def _do():
        conn = _get_index_conn(root)
        _index_upsert_run(conn, run)
        conn.commit()
    _index_safe_write(_do, root, run_id=run.id)


def index_remove_run(run_id, root=None):
    pending = getattr(_index_local, 'pending_writes', None)
    if pending is not None:
        pending[run_id] = {'_delete': True}
        return
    def _do():
        conn = _get_index_conn(root)
        conn.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))
        conn.commit()
    _index_safe_write(_do, root, run_id=run_id)


def index_get_flags(run_id, root=None):
    conn = _get_index_conn(root)
    row = conn.execute(
        "SELECT flags FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    if row and row[0]:
        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            pass
    return None


def index_get_status(run_id, root=None):
    conn = _get_index_conn(root)
    row = conn.execute(
        "SELECT status FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    if row:
        return row[0]
    return None


def index_get_opref(run_id, root=None):
    conn = _get_index_conn(root)
    row = conn.execute(
        "SELECT opref FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    if row:
        return row[0]
    return None


_ATTR_TO_COL = {
    "status": "status",
    "opref": "opref",
    "op_name": "op_name",
    # 'operation' is a computed core attr (guild/index.py) that uses
    # run_util.format_operation — the same function we store into op_name.
    "operation": "op_name",
    "started": "started",
    "initialized": "initialized",
    "label": "label",
    "id": "run_id",
    "run": "run_id",
}

# Computed core attrs from guild/index.py _run_core_attrs that have no
# direct index column. Returning None from _valref_to_sql for these forces
# index_query_runs to bail out so filter_util.filtered_runs falls back to
# the FS path, where these attrs resolve correctly.
_CORE_ATTRS_UNINDEXABLE = frozenset({
    "op", "op_model", "from", "short_id",
    "sourcecode", "stopped", "time",
})


def _flag_col(name):
    return f"json_extract(CASE WHEN flags != '' AND flags IS NOT NULL THEN flags ELSE '{{}}' END, '$.{name}')"


def _valref_to_sql(valref):
    if valref.startswith("attr:"):
        name = valref[5:]
        col = _ATTR_TO_COL.get(name)
        if col:
            return col, []
        return None, None
    if valref.startswith("flag:"):
        return _flag_col(valref[5:]), []
    if valref.startswith("scalar:"):
        return None, None
    col = _ATTR_TO_COL.get(valref)
    if col:
        return col, []
    if valref in _CORE_ATTRS_UNINDEXABLE:
        return None, None
    return _flag_col(valref), []


def _compile_filter_node(node):
    from guild import filter as filterlib

    if isinstance(node, filterlib.RunTest):
        col, params = _valref_to_sql(node.run_valref)
        if col is None:
            return None, None
        target_val = node.target_expr.val
        op_map = {
            "=": "=", " is ": "=",
            "!=": "!=", "<>": "!=", " is not ": "!=",
            "<": "<", "<=": "<=",
            ">": ">", ">=": ">=",
        }
        sql_op = op_map.get(node.cmp_desc)
        if sql_op is None:
            return None, None
        if target_val is None:
            if sql_op == "=":
                return f"({col} IS NULL)", params
            elif sql_op == "!=":
                return f"({col} IS NOT NULL)", params
        params.append(target_val)
        return f"({col} {sql_op} ?)", params

    if isinstance(node, filterlib.InfixOp):
        left_sql, left_params = _compile_filter_node(node.expr1)
        right_sql, right_params = _compile_filter_node(node.expr2)
        if left_sql is None or right_sql is None:
            return None, None
        op = "AND" if node.op_desc == "and" else "OR"
        return f"({left_sql} {op} {right_sql})", left_params + right_params

    if isinstance(node, filterlib.UnaryOp):
        inner_sql, inner_params = _compile_filter_node(node.expr)
        if inner_sql is None:
            return None, None
        return f"(NOT COALESCE({inner_sql}, 0))", inner_params

    if isinstance(node, filterlib.In):
        col, params = _valref_to_sql(node.run_valref)
        if col is None:
            return None, None
        vals = [t.val for t in node.target_expr.terms]
        placeholders = ", ".join("?" for _ in vals)
        params.extend(vals)
        neg = "NOT " if node.not_in else ""
        return f"({col} {neg}IN ({placeholders}))", params

    if isinstance(node, filterlib.Contains):
        col, params = _valref_to_sql(node.run_valref)
        if col is None:
            return None, None
        target_val = node.target_expr.val
        neg = "NOT " if node.not_contains else ""
        params.append(f"%{target_val}%")
        return f"({col} {neg}LIKE ?)", params

    return None, None


def _compile_base_filters(status_include=None, status_exclude=None,
                          op_refs=None, label_terms=None, unlabeled=False,
                          tag_terms=None, started_range=None):
    clauses = []
    params = []

    if status_include:
        placeholders = ", ".join("?" for _ in status_include)
        clauses.append(f"status IN ({placeholders})")
        params.extend(status_include)
    if status_exclude:
        placeholders = ", ".join("?" for _ in status_exclude)
        clauses.append(f"status NOT IN ({placeholders})")
        params.extend(status_exclude)

    if op_refs:
        op_clauses = []
        for ref in op_refs:
            if ref.startswith("^") or ref.endswith("$"):
                return None, None
            op_clauses.append("op_name LIKE ?")
            params.append(f"%{ref}%")
        clauses.append(f"({' OR '.join(op_clauses)})")

    if unlabeled:
        clauses.append("(label IS NULL OR label = '')")
    elif label_terms:
        lbl_clauses = []
        for term in label_terms:
            if term == "-":
                lbl_clauses.append("(label IS NULL OR label = '')")
            else:
                lbl_clauses.append("label LIKE ?")
                params.append(f"%{term}%")
        clauses.append(f"({' OR '.join(lbl_clauses)})")

    if tag_terms:
        tag_clauses = []
        for tag in tag_terms:
            tag_clauses.append("tags LIKE ?")
            params.append(f"%{json.dumps(tag)[1:-1]}%")
        clauses.append(f"({' OR '.join(tag_clauses)})")

    if started_range:
        start, end = started_range
        if start:
            clauses.append("started >= ?")
            params.append(start)
        if end:
            clauses.append("started <= ?")
            params.append(end)

    return " AND ".join(clauses) if clauses else "", params


def index_query_runs(root=None, filter_expr=None, base_sql=None,
                     base_params=None, sort=None):
    root = root or runs_dir()
    conn = _get_index_conn(root)

    where_parts = []
    params = []

    if base_sql:
        where_parts.append(base_sql)
        params.extend(base_params or [])

    if filter_expr:
        expr_sql, expr_params = _compile_filter_node(filter_expr)
        if expr_sql is None:
            return None
        where_parts.append(expr_sql)
        params.extend(expr_params)

    sql = "SELECT run_id FROM runs"
    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)

    _SORT_COL_MAP = {
        "timestamp": "COALESCE(started, initialized)",
        "started": "started",
        "initialized": "initialized",
        "status": "status",
        "opref": "opref",
        "label": "label",
    }
    if sort:
        order_parts = []
        for s in sort:
            desc = s.startswith("-")
            name = s[1:] if desc else s
            col = _SORT_COL_MAP.get(name)
            if col is None:
                return None
            order_parts.append(f"{col} {'DESC' if desc else 'ASC'}")
        sql += " ORDER BY " + ", ".join(order_parts)

    try:
        rows = conn.execute(sql, params).fetchall()
    except sqlite3.OperationalError as e:
        log.debug("Index query operational error, falling back to filesystem: %s", e)
        return None
    except sqlite3.DatabaseError as e:
        if _is_corruption_error(e):
            log.warning(
                "Index DB corruption confirmed during query (%s) - rebuilding",
                e,
            )
            _nuke_index(root)
            return None
        log.debug("Index query DatabaseError, falling back to filesystem: %s", e)
        return None
    if not rows:
        return []
    # One listdir to prune stale rows; avoids O(N) stat calls per query.
    try:
        on_disk = set(os.listdir(root))
    except OSError:
        on_disk = None
    result = []
    stale = []
    for (rid,) in rows:
        if on_disk is None or rid in on_disk:
            result.append(runlib.Run(rid, os.path.join(root, rid)))
        else:
            stale.append((rid,))
    if stale:
        def _do():
            conn.executemany("DELETE FROM runs WHERE run_id = ?",
                             [(rid,) for rid in stale])
            conn.commit()
        _index_safe_write(_do, root)
    return result


def path(*names):
    names = [name for name in names if name]
    return os.path.join(config.guild_home(), *names)


def runs_dir(deleted=False):
    if deleted:
        return trash_dir("runs")
    return path("runs")


def trash_dir(name=None):
    return path("trash", name)


def cache_dir(name=None):
    return path("cache", name)


def pidfile(name):
    return path("proc", name)


def logfile(name):
    return path("log", name)


def remote_dir(name=None):
    # Use directory containing user config to store remote info.
    rest_path = [name] if name else []
    config_path = config.user_config_path()
    if config_path:
        return os.path.join(os.path.dirname(config_path), "remotes", *rest_path)
    return path("remotes", name)


def runs(root=None, sort=None, filter=None, force_root=False, base_runs=None):
    filter = filter or (lambda _: True)
    all_runs = (
        _all_runs_f(root, force_root) if base_runs is None  #
        else lambda: base_runs
    )
    runs = [run for run in all_runs() if filter(run)]
    if sort:
        runs = sorted(runs, key=_run_sort_key(sort))
    return runs


def _all_runs_f(root, force_root):
    root = root or runs_dir()
    if force_root:
        return _default_all_runs_f(root)

    return util.find_apply(
        [
            _zipfile_all_runs_f,
            _runs_under_parent_f,
            _default_all_runs_f,
        ],
        root,
    )


def _default_all_runs_f(root):
    return lambda: _all_runs(root)


def _zipfile_all_runs_f(root):
    if not root or not root.lower().endswith(".zip"):
        return None
    from . import run_zip_proxy

    def f():
        try:
            return run_zip_proxy.all_runs(root)
        except Exception as e:
            if log.getEffectiveLevel() <= logging.DEBUG:
                log.exception("getting runs for zip file %s", root)
            log.error("cannot read from %s: %s", root, e)
            return []

    return f


def _runs_under_parent_f(root):
    runs_parent = os.getenv("GUILD_RUNS_PARENT")
    if not runs_parent:
        return None
    log.debug("limitting to runs under parent %s", runs_parent)
    return lambda: _runs_for_parent(runs_parent, root)


def _runs_for_parent(parent, root):
    parent_path = os.path.join(root, parent)
    try:
        names = os.listdir(parent_path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
        return []
    else:
        return _runs_for_parent_links(parent_path, names, root)


def _runs_for_parent_links(parent_path, names, runs_dir):
    real_paths = [util.realpath(os.path.join(parent_path, name)) for name in names]
    return [
        runlib.for_dir(path) for path in real_paths
        if _is_parent_run_path(path, runs_dir)
    ]


def _is_parent_run_path(path, runs_dir):
    return util.compare_paths(os.path.dirname(path), runs_dir)


def run_filter(name, *args):
    if name.startswith("!"):
        name = name[1:]
        maybe_negate = lambda f: lambda r: not f(r)
    else:
        maybe_negate = lambda f: f
    if name == "true":
        filter = lambda _: True
    elif name == "attr":
        name, expected = args
        filter = lambda r: _run_attr(r, name) == expected
    elif name == "all":
        (filters,) = args
        filter = lambda r: all((f(r) for f in filters))
    elif name == "any":
        (filters,) = args
        filter = lambda r: any((f(r) for f in filters))
    else:
        raise ValueError(name)
    return maybe_negate(filter)


def _all_runs(root):
    return [runlib.Run(name, path) for name, path in _iter_dirs(root)]


def iter_run_dirs(root=None):
    return _iter_dirs(root or runs_dir())


def _fs_only_iter_runs(root):
    """Pure-FS scan that never touches the index lock.

    Use as the last-resort fallback when a lock timeout has already occurred
    on the read path; re-entering `_get_index_conn` would just wait on the
    same contended lock.
    """
    try:
        names = os.listdir(root)
    except OSError:
        return
    for name in names:
        if len(name) != 32:
            continue
        path = os.path.join(root, name)
        if _opref_exists(path):
            yield name, path


def _iter_dirs(root):
    try:
        conn = _get_index_conn(root)
        rows = conn.execute("SELECT run_id FROM runs").fetchall()
        if rows:
            try:
                on_disk = set(os.listdir(root))
            except OSError:
                on_disk = None
            stale = []
            for (name,) in rows:
                if on_disk is None or name in on_disk:
                    yield name, os.path.join(root, name)
                else:
                    stale.append((name,))
            if stale:
                def _do():
                    conn.executemany(
                        "DELETE FROM runs WHERE run_id = ?", stale
                    )
                    conn.commit()
                _index_safe_write(_do, root)
            return
    except sqlite3.DatabaseError as e:
        if _is_corruption_error(e):
            _nuke_index(root)
    except Exception:
        pass
    try:
        names = os.listdir(root)
    except OSError:
        names = []
    for name in names:
        path = os.path.join(root, name)
        if _opref_exists(path):
            yield name, path


def _opref_exists(run_dir):
    opref_path = os.path.join(run_dir, ".guild", "opref")
    return os.path.exists(opref_path)


def _run_sort_key(sort):
    return functools.cmp_to_key(lambda x, y: _run_cmp(x, y, sort))


def _run_cmp(x, y, sort):
    for attr in sort:
        attr_cmp = _run_attr_cmp(x, y, attr)
        if attr_cmp != 0:
            return attr_cmp
    return 0


def _run_attr_cmp(x, y, attr):
    if attr.startswith("-"):
        attr = attr[1:]
        rev = -1
    else:
        rev = 1
    x_val = _run_attr(x, attr)
    if x_val is None:
        return -rev
    y_val = _run_attr(y, attr)
    if y_val is None:
        return rev
    return rev * ((x_val > y_val) - (x_val < y_val))


def _run_attr(run, name):
    if name in runlib.Run.__properties__:
        return getattr(run, name)
    return run.get(name)


def delete_runs(runs, permanent=False):
    to_remove = []
    to_register = []  # (run_id, dest) for soft-delete trash index
    for run in runs:
        src = run.dir
        if permanent:
            _delete_run(src)
            to_remove.append(run.id)
        else:
            dest = os.path.join(runs_dir(deleted=True), run.id)
            _move(src, dest)
            to_remove.append(run.id)
            to_register.append((run.id, dest))
    with index_batch_writes():
        for run_id in to_remove:
            index_remove_run(run_id)
    if to_register:
        trash_root = runs_dir(deleted=True)
        with index_batch_writes(root=trash_root):
            for run_id, dest in to_register:
                index_register_run(runlib.Run(run_id, dest), root=trash_root)


def purge_runs(runs):
    for run in runs:
        _delete_run(run.dir)


def _delete_run(src):
    assert src and src != os.path.sep, src
    assert src.startswith(runs_dir()) or src.startswith(runs_dir(deleted=True)), src
    log.debug("deleting %s", src)
    shutil.rmtree(src)


def _move(src, dest):
    util.ensure_dir(os.path.dirname(dest))
    log.debug("moving %s to %s", src, dest)
    if os.path.exists(dest):
        _move_to_backup(dest)
    shutil.move(src, dest)


def _move_to_backup(path):
    dir = os.path.dirname(path)
    prefix = f"{os.path.basename(path)}_"
    backup = tempfile.NamedTemporaryFile(prefix=prefix, dir=dir, delete=True)
    log.warning("%s exists, moving to %s", path, backup.name)
    backup.close()
    shutil.move(path, backup.name)


def restore_runs(runs):
    to_remove_from_trash = []
    to_register = []
    for run in runs:
        src = os.path.join(run.dir)
        dest = os.path.join(runs_dir(), run.id)
        if util.compare_paths(src, dest):
            log.warning("%s is already restored, skipping", run.id)
            continue
        _move(src, dest)
        to_remove_from_trash.append(run.id)
        to_register.append(runlib.Run(run.id, dest))
    if to_remove_from_trash:
        trash_root = runs_dir(deleted=True)
        with index_batch_writes(root=trash_root):
            for run_id in to_remove_from_trash:
                index_remove_run(run_id, trash_root)
    if to_register:
        with index_batch_writes():
            for restored_run in to_register:
                index_register_run(restored_run)


def find_runs(run_id_prefix, root=None):
    root = root or runs_dir()
    try:
        conn = _get_index_conn(root)
        if len(run_id_prefix) == 32:
            row = conn.execute(
                "SELECT run_id FROM runs WHERE run_id = ?",
                (run_id_prefix,),
            ).fetchone()
            if row:
                return iter([(row[0], os.path.join(root, row[0]))])
            return iter([])
        rows = conn.execute(
            "SELECT run_id FROM runs WHERE run_id LIKE ?",
            (run_id_prefix + "%",),
        ).fetchall()
        return ((rid, os.path.join(root, rid)) for (rid,) in rows)
    except sqlite3.DatabaseError as e:
        if _is_corruption_error(e):
            _nuke_index(root)
    except Exception:
        pass
    return (
        (name, path) for name, path in _fs_only_iter_runs(root)
        if name.startswith(run_id_prefix)
    )


def get_run(run_id, root=None):
    root = root or runs_dir()
    try:
        conn = _get_index_conn(root)
        row = conn.execute(
            "SELECT run_id FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row:
            return runlib.Run(run_id, os.path.join(root, run_id))
    except sqlite3.DatabaseError as e:
        if _is_corruption_error(e):
            _nuke_index(root)
    except Exception:
        pass
    path = os.path.join(root, run_id)
    if os.path.exists(path):
        return runlib.Run(run_id, path)
    raise LookupError(run_id)
