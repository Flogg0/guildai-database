"""Tests for the index read path used by `guild compare` and friends.

The contract under test: a run that the dirty-marker protocol has not
flagged is answered entirely from the index, and a run it has flagged is
re-read from disk. Reading a clean run must not touch its directory at all --
that is the property that makes a query over tens of thousands of runs on
networked storage viable, and it is invisible to output-only assertions, so
these tests count filesystem access directly.
"""

import builtins
import glob as globmod
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time


def _fresh_guild_home():
    tmpdir = tempfile.mkdtemp(prefix="guild_read_path_")
    os.makedirs(os.path.join(tmpdir, "runs"), exist_ok=True)
    os.environ["GUILD_HOME"] = tmpdir
    for mod in list(sys.modules):
        if mod.startswith("guild"):
            sys.modules.pop(mod, None)
    return tmpdir


def _make_run(runs_dir, run_id, scalars=None, status="completed", attrs=None):
    """Creates a run dir, optionally with logged scalars."""
    run_path = os.path.join(runs_dir, run_id)
    guild_dir = os.path.join(run_path, ".guild")
    attrs_dir = os.path.join(guild_dir, "attrs")
    os.makedirs(attrs_dir, exist_ok=True)
    with open(os.path.join(guild_dir, "opref"), "w") as f:
        f.write("guildfile:/test 0000 test test")
    ts = str(int(time.time() * 1000000))
    for name, val in (("initialized", ts), ("started", ts)):
        with open(os.path.join(attrs_dir, name), "w") as f:
            f.write(val)
    for name, val in (attrs or {}).items():
        with open(os.path.join(attrs_dir, name), "w") as f:
            f.write(val)
    if status == "completed":
        with open(os.path.join(attrs_dir, "exit_status"), "w") as f:
            f.write("0")
        with open(os.path.join(attrs_dir, "stopped"), "w") as f:
            f.write(ts)
    if scalars:
        _write_scalars(guild_dir, scalars)
    return run_path


def _write_scalars(guild_dir, scalars):
    from guild import summary

    writer = summary.SummaryWriter(guild_dir)
    for tag, val, step in scalars:
        writer.add_scalar(tag, val, step)
    writer.close()


class _FSCounter:
    """Counts filesystem access under a directory."""

    def __init__(self, root):
        self.root = root
        self.hits = []
        self._orig = {}

    def _record(self, path):
        try:
            path = os.fspath(path)
        except TypeError:
            return
        if isinstance(path, str) and path.startswith(self.root):
            self.hits.append(path)

    def __enter__(self):
        targets = [
            (os, "stat"), (os, "lstat"), (os, "listdir"), (os, "scandir"),
            (os, "walk"), (os, "open"), (builtins, "open"), (globmod, "glob"),
        ]
        for obj, name in targets:
            orig = getattr(obj, name)
            self._orig[(obj, name)] = orig

            def wrapper(*args, _orig=orig, **kw):
                if args:
                    self._record(args[0])
                return _orig(*args, **kw)

            setattr(obj, name, wrapper)
        return self

    def __exit__(self, *exc):
        for (obj, name), orig in self._orig.items():
            setattr(obj, name, orig)
        return False


def _index_and_register(runs_dir, run_ids):
    """Registers runs in the run index, as the headnode path would."""
    from guild import run as runlib
    from guild import var as gvar

    for run_id in run_ids:
        run = runlib.Run(run_id, os.path.join(runs_dir, run_id))
        gvar.index_register_run(run, root=runs_dir)


def _fresh_runs(runs_dir, run_ids):
    """Run objects with no cached index row, as a new process would build."""
    from guild import run as runlib

    return [runlib.Run(r, os.path.join(runs_dir, r)) for r in run_ids]


def test_clean_run_is_read_without_touching_its_directory():
    """The core property: a run the markers have not flagged costs no
    filesystem access on a refresh, however many times it is queried."""
    tmpdir = _fresh_guild_home()
    runs_dir = os.path.join(tmpdir, "runs")
    try:
        from guild import index as indexlib

        run_id = "a" * 32
        run_path = _make_run(runs_dir, run_id, scalars=[("loss", 0.5, 1)])
        _index_and_register(runs_dir, [run_id])

        # First refresh scans the run and caches what it finds.
        index = indexlib.RunIndex()
        index.refresh(_fresh_runs(runs_dir, [run_id]))

        # Second refresh must not go near the run dir.
        index2 = indexlib.RunIndex()
        runs = _fresh_runs(runs_dir, [run_id])
        with _FSCounter(run_path) as counter:
            index2.refresh(runs)
            assert index2.run_attr(runs[0], "operation") is not None
            scalars = index2.run_scalars(runs[0])

        assert not counter.hits, (
            f"clean run touched its directory {len(counter.hits)} time(s): "
            f"{counter.hits[:5]}"
        )
        assert [s["tag"] for s in scalars] == ["loss"], scalars
        assert scalars[0]["last_val"] == 0.5, scalars
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_dirty_run_is_reread():
    """The other half: once the markers flag a run, its new events land."""
    tmpdir = _fresh_guild_home()
    runs_dir = os.path.join(tmpdir, "runs")
    try:
        from guild import index as indexlib
        from guild import var as gvar

        run_id = "b" * 32
        run_path = _make_run(runs_dir, run_id, scalars=[("loss", 0.5, 1)])
        _index_and_register(runs_dir, [run_id])

        index = indexlib.RunIndex()
        index.refresh(_fresh_runs(runs_dir, [run_id]))

        # New events arrive without the run being marked dirty.
        _write_scalars(os.path.join(run_path, ".guild"), [("loss", 0.001, 99)])

        index2 = indexlib.RunIndex()
        runs = _fresh_runs(runs_dir, [run_id])
        index2.refresh(runs)
        clean_val = index2.run_scalars(runs[0])[0]["last_val"]
        assert clean_val == 0.5, (
            f"unflagged run should still read as indexed, got {clean_val}"
        )

        # Now the protocol flags it, which re-syncs the row and re-stamps it.
        time.sleep(1.1)  # marker mtime must exceed db mtime (1s resolution)
        gvar._touch_run_dirty_marker(runs_dir, run_id)
        gvar._drop_cached_conn(f"conn_{gvar._index_db_path(runs_dir)}")

        index3 = indexlib.RunIndex()
        runs = _fresh_runs(runs_dir, [run_id])
        index3.refresh(runs)
        dirty_val = index3.run_scalars(runs[0])[0]["last_val"]
        assert abs(dirty_val - 0.001) < 1e-9, (
            f"flagged run should re-read from disk, got {dirty_val}"
        )
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_unreadable_scan_record_rescans_rather_than_failing():
    """run_scan's payload has changed shape between versions. A record this
    version cannot parse must mean 'rescan', never an error."""
    tmpdir = _fresh_guild_home()
    runs_dir = os.path.join(tmpdir, "runs")
    try:
        from guild import index as indexlib

        run_id = "c" * 32
        _make_run(runs_dir, run_id, scalars=[("loss", 0.25, 1)])
        _index_and_register(runs_dir, [run_id])

        index = indexlib.RunIndex()
        index.refresh(_fresh_runs(runs_dir, [run_id]))

        # Overwrite with each historical payload shape plus outright garbage.
        for payload in (
            json.dumps([".guild"]),  # original: bare prefixes
            json.dumps([[".guild", True]]),  # then: (prefix, has_attrs)
            "not json at all",
            json.dumps({"unexpected": "shape"}),
        ):
            db = sqlite3.connect(
                os.path.join(tmpdir, "cache", "runs", indexlib.DB_NAME)
            )
            db.execute(
                "UPDATE run_scan SET prefixes = ? WHERE run = ?", (payload, run_id)
            )
            db.commit()
            db.close()

            index2 = indexlib.RunIndex()
            runs = _fresh_runs(runs_dir, [run_id])
            index2.refresh(runs)
            scalars = index2.run_scalars(runs[0])
            assert scalars and scalars[0]["last_val"] == 0.25, (
                f"payload {payload!r} did not recover: {scalars}"
            )
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_absent_attr_answered_from_index():
    """A run with no `stopped` must be answered from the index, not by
    probing the run dir for a file that is not there."""
    tmpdir = _fresh_guild_home()
    runs_dir = os.path.join(tmpdir, "runs")
    try:
        run_id = "d" * 32
        run_path = _make_run(runs_dir, run_id, status="staged")
        _index_and_register(runs_dir, [run_id])

        runs = _fresh_runs(runs_dir, [run_id])
        with _FSCounter(run_path) as counter:
            assert runs[0].get("stopped") is None
            assert runs[0].get("sourcecode_digest") is None

        assert not counter.hits, (
            f"absent attrs probed the run dir: {counter.hits[:5]}"
        )
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_known_file_digest_matches_listing_digest():
    """Recomputing the digest from recorded filenames must produce exactly
    what listing the directory would. If the two ever diverge, every run
    looks permanently stale and is re-read on every query."""
    tmpdir = _fresh_guild_home()
    runs_dir = os.path.join(tmpdir, "runs")
    try:
        from guild import tfevent

        run_id = "f" * 32
        run_path = _make_run(
            runs_dir, run_id, scalars=[("loss", 0.5, 1), ("acc", 0.9, 1)]
        )
        # A second events file, so ordering across files is exercised too.
        _write_scalars(os.path.join(run_path, ".guild"), [("loss", 0.4, 2)])

        scanned = tfevent.scan_event_dirs(run_path)
        assert scanned, "no event dirs found"
        for path, _has_attrs, names in scanned:
            assert len(names) >= 1, names
            from_listing = tfevent._event_files_digest(path)
            from_names = tfevent._digest_for_known_files(path, names)
            assert from_names == from_listing, (
                f"{path}: known-file digest {from_names} != "
                f"listing digest {from_listing}"
            )

        # A file that no longer exists must fall back, not fabricate a digest.
        missing = tfevent._digest_for_known_files(
            scanned[0][0], list(scanned[0][2]) + ["events.out.tfevents.gone"]
        )
        assert missing is None, f"expected fallback signal, got {missing}"
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_digest_changes_when_an_event_file_grows():
    """A run that appends to an existing events file must read as changed.

    The filename set is unchanged in that case, so only the recorded size
    distinguishes it - and a digest that ignored size would silently serve
    stale scalars for any run written to in place.
    """
    tmpdir = _fresh_guild_home()
    runs_dir = os.path.join(tmpdir, "runs")
    try:
        from guild import tfevent

        run_id = "0" * 32
        run_path = _make_run(runs_dir, run_id, scalars=[("loss", 0.5, 1)])
        path, _has_attrs, names = tfevent.scan_event_dirs(run_path)[0]

        before = tfevent._digest_for_known_files(path, names)
        with open(os.path.join(path, names[0]), "ab") as f:
            f.write(b"\x00" * 64)
        after = tfevent._digest_for_known_files(path, names)

        assert before != after, (
            f"digest unchanged after the events file grew ({before})"
        )
        # And it still agrees with what a fresh listing would produce.
        assert after == tfevent._event_files_digest(path)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_run_without_index_row_still_reads_from_disk():
    """The index is an accelerator, not a prerequisite: a run it has never
    seen must still be read correctly."""
    tmpdir = _fresh_guild_home()
    runs_dir = os.path.join(tmpdir, "runs")
    try:
        from guild import index as indexlib

        run_id = "e" * 32
        _make_run(runs_dir, run_id, scalars=[("loss", 0.75, 3)])
        # Deliberately not registered in the run index.

        index = indexlib.RunIndex()
        runs = _fresh_runs(runs_dir, [run_id])
        index.refresh(runs)
        scalars = index.run_scalars(runs[0])
        assert scalars and scalars[0]["last_val"] == 0.75, scalars
        assert index.run_attr(runs[0], "status") is not None
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
