"""Tests for the index read path used by `guild compare` and friends.

The contract under test: a run that the dirty-marker protocol has not
flagged is answered entirely from the index, and a run it has flagged is
re-read from disk. Reading a clean run must not touch its directory at all --
that is the property that makes a query over tens of thousands of runs on
networked storage viable, and it is invisible to output-only assertions, so
these tests count filesystem access directly.
"""

import builtins
import collections
import glob as globmod
import io
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
        self.calls = []
        self._orig = {}

    def _record(self, path, name=None):
        try:
            path = os.fspath(path)
        except TypeError:
            return
        if isinstance(path, str) and path.startswith(self.root):
            self.hits.append(path)
            self.calls.append((name, path))

    def __enter__(self):
        targets = [
            (os, "stat"), (os, "lstat"), (os, "listdir"), (os, "scandir"),
            (os, "walk"), (os, "open"), (builtins, "open"), (io, "open"),
            (globmod, "glob"),
        ]
        for obj, name in targets:
            orig = getattr(obj, name)
            self._orig[(obj, name)] = orig

            def wrapper(*args, _orig=orig, _name=name, **kw):
                if args:
                    self._record(args[0], _name)
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
        # Flags are given because an attr the index stores as empty still
        # falls through to the run dir - the index records "" for a run that
        # had none when indexed, which is not proof it has none now.
        run_path = _make_run(
            runs_dir,
            run_id,
            scalars=[("loss", 0.5, 1)],
            attrs={"flags": "seed: 1\n", "label": "test-label\n"},
        )
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


def test_absent_init_attr_answered_from_index():
    """An absent init-time attr is answered from the index without probing.

    Only these attrs qualify. They are written once when the run is
    initialized and never mutated, and the index records their absence
    affirmatively (an empty string, distinct from NULL). For anything else a
    NULL column means "not captured", not "absent" -- a row can be written
    before an attr exists -- so a miss must still go to the run dir.
    """
    tmpdir = _fresh_guild_home()
    runs_dir = os.path.join(tmpdir, "runs")
    try:
        run_id = "d" * 32
        run_path = _make_run(runs_dir, run_id, status="staged")
        _index_and_register(runs_dir, [run_id])

        runs = _fresh_runs(runs_dir, [run_id])
        with _FSCounter(run_path) as counter:
            assert runs[0].get("sourcecode_digest") is None
            assert runs[0].get("opdef_attrs") is None
            assert runs[0].get("compare") is None

        assert not counter.hits, (
            f"absent init attrs probed the run dir: {counter.hits[:5]}"
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


def test_retained_marker_survives_unrelated_index_writes():
    """A marker stays pending until its own run has been re-read for it.

    _do_delta_sync deliberately keeps the marker for a run with no definitive
    status, so the sync retries once the run finishes. That retry only
    happens if freshness is judged per run: when it was judged against the
    index DB file's mtime, a write about any other run retired the marker,
    after which it was never processed again and the run's row stayed stale
    permanently.
    """
    tmpdir = _fresh_guild_home()
    runs_dir = os.path.join(tmpdir, "runs")
    try:
        from guild import run as runlib
        from guild import var as gvar

        inflight, other = "1" * 32, "2" * 32
        # In flight: no exit_status, so a delta sync must skip and retry it.
        _make_run(runs_dir, inflight, status="running")
        _make_run(runs_dir, other, status="completed")
        _index_and_register(runs_dir, [inflight, other])

        attrs = os.path.join(runs_dir, inflight, ".guild", "attrs")
        with open(os.path.join(attrs, "label"), "w") as f:
            f.write("written-by-worker\n")
        time.sleep(1.1)  # marker mtime resolution
        gvar._touch_run_dirty_marker(runs_dir, inflight)
        gvar._drop_cached_conn(f"conn_{gvar._index_db_path(runs_dir)}")

        # A read syncs, skips the in-flight run, and keeps its marker.
        gvar._get_index_conn(runs_dir)
        assert os.path.exists(
            os.path.join(gvar._dirty_runs_dir(runs_dir), inflight)
        ), "marker for an in-flight run should be retained"

        # Index activity about an unrelated run.
        time.sleep(1.1)
        gvar.index_update_status(
            runlib.Run(other, os.path.join(runs_dir, other)),
            "completed",
            root=runs_dir,
        )

        # The run finishes for real.
        with open(os.path.join(attrs, "label"), "w") as f:
            f.write("final-label\n")
        with open(os.path.join(attrs, "exit_status"), "w") as f:
            f.write("0")
        gvar._drop_cached_conn(f"conn_{gvar._index_db_path(runs_dir)}")

        conn = gvar._get_index_conn(runs_dir)
        label = conn.execute(
            "SELECT label FROM runs WHERE run_id = ?", (inflight,)
        ).fetchone()[0]
        assert label == "final-label", (
            f"retained marker was not honored after unrelated index writes; "
            f"index still reports {label!r}"
        )
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


def test_scan_event_dirs_matches_os_walk():
    """scan_event_dirs walks with directory entries rather than os.walk plus
    an lstat per subdir. What it finds -- which linked dirs it follows, and
    the order dirs come back in -- must be what the os.walk version found,
    apart from .guild/attrs and .guild/proto, which it no longer descends
    into."""
    tmpdir = _fresh_guild_home()
    runs_dir = os.path.join(tmpdir, "runs")
    try:
        from guild import tfevent

        run_path = _make_run(runs_dir, "1" * 32, scalars=[("loss", 0.5, 1)])
        _write_scalars(os.path.join(run_path, "logs", "train"), [("acc", 0.9, 1)])
        _write_scalars(os.path.join(run_path, "logs", "eval"), [("acc", 0.8, 1)])
        # A linked dir that is a run is followed; any other linked dir is not.
        other_run = _make_run(runs_dir, "2" * 32, scalars=[("loss", 0.1, 1)])
        os.symlink(other_run, os.path.join(run_path, "upstream"))
        not_a_run = os.path.join(tmpdir, "elsewhere")
        _write_scalars(not_a_run, [("loss", 0.2, 1)])
        os.symlink(not_a_run, os.path.join(run_path, "elsewhere"))
        os.makedirs(os.path.join(run_path, ".guild", "proto", ".guild"))

        expected = []
        for root, dirs, files in os.walk(run_path, followlinks=True):
            tfevent._del_non_run_linked_dirs(dirs, root)
            if root == os.path.join(run_path, ".guild"):
                dirs[:] = [name for name in dirs if name not in ("attrs", "proto")]
            event_files = [name for name in files if tfevent._is_event_file(name)]
            if event_files:
                expected.append(
                    (
                        root,
                        any(tfevent._is_summary_attrs(name) for name in files),
                        sorted(event_files),
                    )
                )

        found, guild_names = tfevent.scan_run_dir(run_path)
        assert found == expected, (found, expected)
        assert tfevent.scan_event_dirs(run_path) == found
        found_dirs = {os.path.relpath(path, run_path) for path, _, _ in found}
        assert found_dirs == {
            ".guild", "logs/train", "logs/eval", "upstream/.guild"
        }, found_dirs
        assert sorted(guild_names) == sorted(
            os.listdir(os.path.join(run_path, ".guild"))
        ), guild_names
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_first_read_of_a_run_lists_each_dir_once():
    """Reading a run the index hasn't scanned yet has to walk it and read
    its events, but no more than once: one listing per dir (never
    .guild/attrs, which holds only attr files), no lstat per subdir, no
    batch check beyond the .guild listing, no second listing to find the
    event files the walk just found, and one open per event file. On
    networked storage each of those is a round-trip, paid for every run the
    first time it is compared."""
    tmpdir = _fresh_guild_home()
    runs_dir = os.path.join(tmpdir, "runs")
    try:
        from guild import index as indexlib

        run_id = "9" * 32
        run_path = _make_run(runs_dir, run_id, scalars=[("loss", 0.5, 1)])
        _index_and_register(runs_dir, [run_id])

        index = indexlib.RunIndex()
        runs = _fresh_runs(runs_dir, [run_id])
        with _FSCounter(run_path) as counter:
            # As compare does: drop batch runs, then refresh the rest.
            index.batch_runs(runs)
            index.refresh(runs)
            scalars = index.run_scalars(runs[0])
        assert [s["last_val"] for s in scalars] == [0.5], scalars

        guild_dir = os.path.join(run_path, ".guild")
        listings = collections.Counter(
            path for name, path in counter.calls
            if name in ("listdir", "scandir", "walk", "glob")
        )
        assert listings == {run_path: 1, guild_dir: 1}, listings
        lstats = [path for name, path in counter.calls if name == "lstat"]
        assert not lstats, lstats
        proto = os.path.join(guild_dir, "proto")
        assert not [c for c in counter.calls if c[1] == proto], counter.calls
        event_opens = [
            path for name, path in counter.calls
            if name == "open" and ".tfevents." in path
        ]
        assert len(event_opens) == 1, event_opens
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_deleting_a_run_drops_it_from_the_scalars_index():
    """Nothing else removes a run's rows from the scalars index, which
    otherwise keeps every deleted run's scalars and scan for good."""
    tmpdir = _fresh_guild_home()
    runs_dir = os.path.join(tmpdir, "runs")
    try:
        from guild import index as indexlib
        from guild import var as gvar

        ids = ["7" * 32, "8" * 32, "6" * 32]
        for run_id in ids:
            _make_run(runs_dir, run_id, scalars=[("loss", 0.5, 1)])
        _index_and_register(runs_dir, ids)
        indexlib.RunIndex().refresh(_fresh_runs(runs_dir, ids))
        tables = ("scalar", "scalar_source", "run_scan")

        def indexed_runs():
            db = sqlite3.connect(os.path.join(gvar.cache_dir("runs"), indexlib.DB_NAME))
            try:
                return {
                    table: {row[0] for row in db.execute(f"SELECT run FROM {table}")}
                    for table in tables
                }
            finally:
                db.close()

        assert indexed_runs() == {table: set(ids) for table in tables}

        trashed, removed, kept = _fresh_runs(runs_dir, ids)
        gvar.delete_runs([trashed])
        gvar.delete_runs([removed], permanent=True)
        assert indexed_runs() == {table: {kept.id} for table in tables}
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_batch_check_comes_from_the_walk():
    """A batch run is one with .guild/proto. For a run not yet scanned, the
    walk that the refresh needs anyway lists .guild, so batch_runs asks it
    rather than looking proto up -- and what it records is the same."""
    tmpdir = _fresh_guild_home()
    runs_dir = os.path.join(tmpdir, "runs")
    try:
        from guild import index as indexlib

        batch_id, trial_id = "4" * 32, "5" * 32
        batch_path = _make_run(runs_dir, batch_id)
        os.makedirs(os.path.join(batch_path, ".guild", "proto", ".guild"))
        _make_run(runs_dir, trial_id, scalars=[("loss", 0.5, 1)])
        _index_and_register(runs_dir, [batch_id, trial_id])

        index = indexlib.RunIndex()
        runs = _fresh_runs(runs_dir, [batch_id, trial_id])
        with _FSCounter(runs_dir) as counter:
            batch = index.batch_runs(runs)
            index.refresh(runs)
        assert batch == {batch_id}, batch
        proto_lookups = [c for c in counter.calls if c[1].endswith("/.guild/proto")]
        assert not proto_lookups, proto_lookups

        recorded = indexlib.RunIndex()._recorded_scans(runs)
        assert recorded[batch_id][2] is True, recorded[batch_id]
        assert recorded[trial_id][2] is False, recorded[trial_id]
        # Served from the record from now on.
        assert indexlib.RunIndex().batch_runs(_fresh_runs(runs_dir, [batch_id, trial_id])) == {batch_id}
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _event_file(guild_dir):
    [name] = [name for name in os.listdir(guild_dir) if ".tfevents." in name]
    return os.path.join(guild_dir, name)


def _events_as_read(path):
    """(events read by Guild's reader, events read by TB's), serialized."""
    from guild import tensorboard_util
    from tensorboard.backend.event_processing import event_file_loader

    ours = [e.SerializeToString() for e in tensorboard_util._read_event_file(path)]
    tb = [
        e.SerializeToString()
        for e in event_file_loader.LegacyEventFileLoader(path).Load()
    ]
    return ours, tb


def test_event_file_reader_reads_what_tensorboard_reads():
    """_read_event_file stands in for TB's stub reader, so it must yield the
    same events -- including from files TB stops reading part-way: one a run
    is still writing, which ends mid-record, and one with a record that fails
    its CRC check."""
    tmpdir = _fresh_guild_home()
    try:
        guild_dir = os.path.join(tmpdir, "events")
        _write_scalars(guild_dir, [("loss", 1.0 / step, step) for step in range(1, 11)])
        path = _event_file(guild_dir)
        with open(path, "rb") as f:
            data = f.read()

        ours, tb = _events_as_read(path)
        assert len(tb) == 11, len(tb)  # file version event + 10 scalars
        assert ours == tb

        # Cut the last record short, as an interrupted write leaves it.
        with open(path, "wb") as f:
            f.write(data[:-7])
        ours, tb = _events_as_read(path)
        assert len(tb) == 10, len(tb)
        assert ours == tb

        # Corrupt a byte in the middle of the file: TB stops at the record
        # holding it, and so must this.
        corrupt = bytearray(data)
        corrupt[len(data) // 2] ^= 0xFF
        with open(path, "wb") as f:
            f.write(bytes(corrupt))
        ours, tb = _events_as_read(path)
        assert 0 < len(tb) < 11, len(tb)
        assert ours == tb
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
