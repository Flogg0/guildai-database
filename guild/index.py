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

import json
import logging
import os
import sqlite3

from guild import run_util
from guild import tfevent
from guild import util
from guild import var

log = logging.getLogger("guild")

VERSION = 1
DB_NAME = f"index_v{VERSION}.db"

CORE_ATTRS = {
    "id",
    "run",
    "operation",
    "from",
    "op",
    "op_model",
    "short_id",
    "sourcecode",
    "started",
    "stopped",
    "status",
    "time",
}


# SQLite's default parameter limit is 999; stay under it when expanding a
# run list into an IN clause.
_ID_CHUNK = 500


def _id_chunks(runs):
    ids = [run.id for run in runs]
    for i in range(0, len(ids), _ID_CHUNK):
        yield ids[i:i + _ID_CHUNK]


class AttrReader:
    def __init__(self):
        self._data = {}

    def refresh(self, runs, event_dirs=None):
        self._data = _runs_attr_data(runs, event_dirs or {})

    def read(self, run, attr):
        run_data = self._data.get(run.id)
        if run_data is None:
            # Require a refresh as an indication user wants run attrs.
            return None
        try:
            return run_data[attr]
        except KeyError:
            # Fallback on run to support all run attributes
            return run.get(attr)

    def read_all(self, run):
        return self._data.get(run.id)


def _runs_attr_data(runs, event_dirs):
    return {run.id: _run_attr_data(run, event_dirs.get(run.id)) for run in runs}


def _run_attr_data(run, event_dirs=None):
    # Order is important - core overrides user-defined attrs
    with run.pinned_attrs():
        return {
            **_run_opdef_attrs(run),
            **_run_logged_attrs(run, event_dirs),
            **_run_core_attrs(run),
        }


def _run_opdef_attrs(run):
    return run.get("opdef_attrs") or {}


def _run_logged_attrs(run, event_dirs=None):
    attrs = {}
    if event_dirs is not None and not event_dirs:
        # The index knows this run has no event dirs - nothing to read, and
        # no reason to look.
        return attrs
    for path, reader in tfevent.attr_readers(run.dir, dirs=event_dirs):
        prefix = _attr_prefix(path, run.dir)
        for name, val in reader:
            attrs[_attr_name(prefix, name)] = val
    return attrs


def _attr_prefix(attrs_path, root):
    rel_path = os.path.relpath(attrs_path, root)
    if rel_path == ".":
        return ""
    return rel_path.replace(os.sep, "/")


def _attr_name(prefix, name):
    return f"{prefix}#{name}" if prefix and prefix != ".guild" else name


def _run_core_attrs(run):
    opref = run.opref
    started = run.get("started")
    stopped = run.get("stopped")
    status = run.status
    # The run index already stores the formatted operation (it computes the
    # same run_util.format_operation when indexing the run). Reusing it avoids
    # re-deriving the value here, which stats .guild/proto to resolve a batch
    # run's proto description.
    operation = run.indexed_op_name or run_util.format_operation(run)
    data = {
        "id": run.id,
        "run": run.short_id,
        "operation": operation,
        "from": run_util.format_pkg_name(run),
        "op": opref.op_name,
        "op_model": opref.model_name,
        "short_id": run.short_id,
        "sourcecode": util.short_digest(run.get("sourcecode_digest")),
        "started": util.format_timestamp(started),
        "stopped": util.format_timestamp(stopped),
        "status": status,
        "time": run_util.calc_run_duration(status, started, stopped),
    }
    assert sorted(data) == sorted(CORE_ATTRS), data
    return data


class FlagReader:
    def __init__(self):
        self._data = {}

    def refresh(self, runs):
        self._data = {run.id: run.get("flags", {}) for run in runs}

    def read(self, run, flag):
        run_data = self._data.get(run.id)
        if run_data is None:
            # Require a refresh as an indication user wants run flags.
            return None
        return run_data.get(flag)


class ScalarReader:
    _col_index_map = {
        ("first", False): 3,
        ("first", True): 4,
        ("last", False): 5,
        ("last", True): 6,
        ("min", False): 7,
        ("min", True): 8,
        ("max", False): 9,
        ("max", True): 10,
        ("avg", False): 11,
        ("total", False): 12,
        ("count", False): 13,
    }

    def __init__(self, db):
        self._db = db
        self._source_digests = None
        self._preloaded = None

    def refresh(self, runs, event_dirs=None):
        event_dirs = event_dirs or {}
        self._preload_source_digests(runs)
        dirty = False
        for run in runs:
            dirs = event_dirs.get(run.id)
            if dirs is None:
                readers = tfevent.scalar_readers(run.dir)
            elif not dirs:
                continue
            else:
                readers = tfevent.scalar_readers_for(dirs)
            for path, cur_digest, reader in readers:
                if self._maybe_refresh_run_scalars(run, path, cur_digest, reader):
                    dirty = True
        if dirty:
            self._db.commit()
            self._source_digests = None
        self._preload_scalars(runs)

    def _preload_source_digests(self, runs):
        """Load every refreshed run's source digests in one query.

        Replaces a per-run SELECT in _scalar_source_digest. Each of those
        statements also cost SQLite a hot-journal probe (a stat of the
        -journal and -wal paths), so on networked storage the per-run query
        was several round-trips, not one.
        """
        self._source_digests = {}
        for chunk in _id_chunks(runs):
            placeholders = ", ".join("?" for _ in chunk)
            rows = self._db.execute(
                f"SELECT run, prefix, path_digest FROM scalar_source "
                f"WHERE run IN ({placeholders})",
                chunk,
            )
            for run_id, prefix, digest in rows:
                self._source_digests[(run_id, prefix)] = digest

    def _preload_scalars(self, runs):
        """Load every refreshed run's scalars in one query.

        `read` and `iter_scalars` are called per run per column, so serving
        them from memory removes the bulk of this DB's query traffic.
        """
        self._preloaded = {run.id: [] for run in runs}
        for chunk in _id_chunks(runs):
            placeholders = ", ".join("?" for _ in chunk)
            cur = self._db.execute(
                f"SELECT * FROM scalar WHERE run IN ({placeholders})",
                chunk,
            )
            cols = [col[0] for col in cur.description]
            for row in cur.fetchall():
                s = dict(zip(cols, row))
                self._preloaded.setdefault(s["run"], []).append(s)

    def _maybe_refresh_run_scalars(self, run, path, cur_digest, reader):
        log.debug("Found events in %s (digest %s)", path, cur_digest)
        prefix = _scalar_prefix(path, run.dir)
        last_digest = self._scalar_source_digest(run.id, prefix)
        if cur_digest != last_digest:
            log.debug(
                "Last digest for %s (%s) is stale, refreshing scalars",
                path,
                last_digest or 'unset',
            )
            self._refresh_run_scalars(run, prefix, cur_digest, last_digest, reader)
            return True
        return False

    def _refresh_run_scalars(self, run, prefix, cur_digest, last_digest, reader):
        summarized = _summarize_scalars(reader)
        if last_digest:
            self._del_scalars(run.id, prefix)
        self._write_summarized(run.id, prefix, summarized)
        self._write_source_digest(run.id, prefix, cur_digest)

    def _scalar_source_digest(self, run_id, prefix):
        if self._source_digests is not None:
            return self._source_digests.get((run_id, prefix))
        cur = self._db.execute(
            """
          SELECT path_digest FROM scalar_source
          WHERE run = ? AND prefix = ?
        """,
            (run_id, prefix),
        )
        row = cur.fetchone()
        if not row:
            return None
        return row[0]

    def _del_scalars(self, run_id, prefix):
        self._db.execute(
            """
          DELETE from scalar
          WHERE run = ? AND prefix = ?
        """,
            (run_id, prefix),
        )

    def _write_summarized(self, run_id, prefix, summarized):
        cur = self._db.cursor()
        for tag in summarized:
            tsum = summarized[tag]
            cur.execute(
                """
              INSERT INTO scalar
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    run_id,
                    prefix,
                    tag,
                    tsum.first_val,
                    tsum.first_step,
                    tsum.last_val,
                    tsum.last_step,
                    tsum.min_val,
                    tsum.min_step,
                    tsum.max_val,
                    tsum.max_step,
                    tsum.avg_val,
                    tsum.total,
                    tsum.count,
                ),
            )

    def _write_source_digest(self, run_id, prefix, path_digest):
        cur = self._db.cursor()
        cur.execute(
            """
          UPDATE scalar_source
            SET path_digest = ?
            WHERE run = ? AND prefix = ?
        """,
            (path_digest, run_id, prefix),
        )
        cur.execute(
            """
          INSERT OR IGNORE INTO scalar_source
          VALUES (?, ?, ?)
        """,
            (run_id, prefix, path_digest),
        )

    def read(self, run, prefix, tag, qual, step):
        col_index = self._read_col_index(qual, step)
        preloaded = self._preloaded.get(run.id) if self._preloaded is not None else None
        if preloaded is not None:
            return self._read_preloaded(preloaded, prefix, tag, qual, step)
        cur = self._db.cursor()
        if prefix is None:
            cur.execute(
                """
              SELECT * FROM scalar
              WHERE run = ? AND tag = ?
            """,
                (run.id, tag),
            )
        else:
            cur.execute(
                """
              SELECT * FROM scalar
              WHERE run = ? AND prefix LIKE ? AND tag = ?
              ORDER BY prefix, tag
            """,
                (run.id, f"{prefix}%", tag),
            )
        row = cur.fetchone()
        if not row:
            return None
        return row[col_index]

    _COL_NAMES = [
        "run", "prefix", "tag", "first_val", "first_step", "last_val",
        "last_step", "min_val", "min_step", "max_val", "max_step", "avg_val",
        "total", "count",
    ]

    def _read_preloaded(self, rows, prefix, tag, qual, step):
        """In-memory equivalent of the `read` SELECTs.

        Match order mirrors the SQL: unprefixed reads take the first row in
        table order, prefixed reads take the first ordered by (prefix, tag).
        """
        col = self._COL_NAMES[self._read_col_index(qual, step)]
        if prefix is None:
            for s in rows:
                if s["tag"] == tag:
                    return s[col]
            return None
        matched = [
            s for s in rows if s["tag"] == tag and str(s["prefix"]).startswith(prefix)
        ]
        if not matched:
            return None
        matched.sort(key=lambda s: (s["prefix"], s["tag"]))
        return matched[0][col]

    def _read_col_index(self, qual, step):
        try:
            return self._col_index_map[(qual or "last", step)]
        except KeyError:
            raise ValueError(
                f"unsupported scalar type qual={qual!r} step={step}"
            ) from None

    def iter_scalars(self, run):
        preloaded = self._preloaded.get(run.id) if self._preloaded is not None else None
        if preloaded is not None:
            for s in sorted(preloaded, key=lambda s: (s["prefix"], s["tag"])):
                yield s
            return
        cur = self._db.execute(
            """
          SELECT * FROM scalar
          WHERE run = ?
          ORDER BY prefix, tag
        """,
            (run.id,),
        )
        for row in cur.fetchall():
            yield {col[0]: row[i] for i, col in enumerate(cur.description)}


def _decode_scan_prefixes(encoded):
    """Decodes a run_scan prefixes payload, or None if unrecognized.

    This is a derived cache and the payload shape has changed between Guild
    versions -- it previously held bare prefixes, then (prefix, has_attrs)
    pairs, and now (prefix, has_attrs, event_filenames). A record this version
    cannot read means "rescan this run", never an error.
    """
    try:
        decoded = json.loads(encoded)
    except (TypeError, ValueError):
        return None
    if not isinstance(decoded, list):
        return None
    for item in decoded:
        if not isinstance(item, list) or len(item) != 3:
            return None
        prefix, _has_attrs, names = item
        if not isinstance(prefix, str) or not isinstance(names, list):
            return None
    return decoded


def _scalar_prefix(scalars_path, root):
    rel_path = os.path.relpath(scalars_path, root)
    if rel_path == ".":
        return ""
    return rel_path.replace(os.sep, "/")


def _summarize_scalars(reader):
    summarized = {}
    for tag, val, step in reader:
        # Don't use dict.setdefault to avoid repeated calls to
        # TagSummary.
        try:
            tag_summary = summarized[tag]
        except KeyError:
            summarized[tag] = tag_summary = TagSummary()
        tag_summary.add(val, step)
    return summarized


class TagSummary:
    def __init__(self):
        self.first_val = None
        self.first_step = None
        self.last_val = None
        self.last_step = None
        self.min_val = None
        self.min_step = None
        self.max_val = None
        self.max_step = None
        self.avg_val = None
        self.total = 0
        self.count = 0

    def add(self, val, step):
        self._set_first(val, step)
        self._set_last(val, step)
        self._set_min(val, step)
        self._set_max(val, step)
        self.total += val
        self.count += 1
        self.avg_val = self.total / self.count

    def _set_first(self, val, step):
        if self.first_step is None or step < self.first_step:
            self.first_val = val
            self.first_step = step

    def _set_last(self, val, step):
        if self.last_step is None or step >= self.last_step:
            self.last_val = val
            self.last_step = step

    def _set_min(self, val, step):
        if self.min_val is None or val < self.min_val:
            self.min_val = val
            self.min_step = step

    def _set_max(self, val, step):
        if self.max_val is None or val > self.max_val:
            self.max_val = val
            self.max_step = step


class RunIndex:
    """Interface for using a run index."""
    def __init__(self, path=None):
        self.path = path or var.cache_dir("runs")
        self._db = self._init_db()
        self._attr_reader = AttrReader()
        self._flag_reader = FlagReader()
        self._scalar_reader = ScalarReader(self._db)

    def _init_db(self):
        db_path = self._db_path()
        util.ensure_dir(os.path.dirname(db_path))
        db = sqlite3.connect(db_path, timeout=300)
        db.execute("PRAGMA busy_timeout=300000")
        db.execute("PRAGMA synchronous=NORMAL")
        _init_run_index_tables(db)
        return db

    def _db_path(self):
        return os.path.join(self.path, DB_NAME)

    def refresh(self, runs, types=None):
        """Refreshes the index for the specified runs.

        `runs` is list of runs or run IDs for each run to refresh.

        `types` is an optional list of data types to refresh.
        """
        needs_events = types is None or "attr" in types or "scalar" in types
        scanned = self._run_event_dirs(runs) if needs_events else {}
        if types is None or "attr" in types:
            # Only dirs that actually hold logged attrs are worth reading.
            attr_dirs = {
                run_id: [path for path, has_attrs, _names in dirs if has_attrs]
                for run_id, dirs in scanned.items()
            }
            self._attr_reader.refresh(runs, attr_dirs)
        if types is None or "flag" in types:
            self._flag_reader.refresh(runs)
        if types is None or "scalar" in types:
            scalar_dirs = {
                run_id: [(path, names) for path, _has_attrs, names in dirs]
                for run_id, dirs in scanned.items()
            }
            self._scalar_reader.refresh(runs, scalar_dirs)

    def batch_runs(self, runs):
        """Returns the ids of `runs` that are batch runs.

        Batch-ness is fixed once a run is created, so it is recorded with the
        run scan and served from there; without that, callers that filter
        batches out stat .guild/proto for every run on every invocation.
        """
        from guild import batch_util

        recorded = self._recorded_scans(runs)
        batch = set()
        for run in runs:
            scan = recorded.get(run.id)
            if scan is not None and scan[0] == run.status:
                if scan[2]:
                    batch.add(run.id)
            elif batch_util.is_batch(run):
                batch.add(run.id)
        return batch

    def _run_event_dirs(self, runs):
        """Maps run id -> list of dirs holding that run's event files.

        Finding those dirs means walking the run directory, which is by far
        the most expensive thing an index refresh does - and its result is
        stable for a run that isn't being written to. So the result is cached
        in the index and keyed on the run's status: any status change (staged
        -> running -> completed) re-walks, and a run whose status is unchanged
        is served from the index without touching the filesystem.

        A run with no event dirs is recorded as such, so "no events" is
        distinguishable from "not yet scanned" and does not re-walk forever.
        """
        from guild import batch_util

        recorded = self._recorded_scans(runs)
        dirs = {}
        new_records = []
        for run in runs:
            status = run.status
            scan = recorded.get(run.id)
            if scan is not None and scan[0] == status:
                dirs[run.id] = [
                    (
                        os.path.join(run.dir, prefix) if prefix else run.dir,
                        has_attrs,
                        names,
                    )
                    for prefix, has_attrs, names in scan[1]
                ]
                continue
            found = tfevent.scan_event_dirs(run.dir)
            dirs[run.id] = found
            new_records.append(
                (
                    run.id,
                    status,
                    json.dumps(
                        [
                            [_scalar_prefix(path, run.dir), has_attrs, names]
                            for path, has_attrs, names in found
                        ]
                    ),
                    1 if batch_util.is_batch(run) else 0,
                )
            )
        if new_records:
            self._write_run_scans(new_records)
        return dirs

    def _recorded_scans(self, runs):
        """Maps run id -> (status, [(prefix, has_attrs)], is_batch)."""
        recorded = {}
        for chunk in _id_chunks(runs):
            placeholders = ", ".join("?" for _ in chunk)
            try:
                rows = self._db.execute(
                    f"SELECT run, status, prefixes, is_batch FROM run_scan "
                    f"WHERE run IN ({placeholders})",
                    chunk,
                )
            except sqlite3.OperationalError:
                return {}
            for run_id, status, prefixes, is_batch in rows:
                decoded = _decode_scan_prefixes(prefixes)
                if decoded is not None:
                    recorded[run_id] = (status, decoded, bool(is_batch))
        return recorded

    def _write_run_scans(self, records):
        try:
            self._db.executemany(
                "INSERT OR REPLACE INTO run_scan "
                "(run, status, prefixes, is_batch) VALUES (?, ?, ?, ?)",
                records,
            )
            self._db.commit()
        except sqlite3.OperationalError as e:
            # A read-only or locked index must not break the read path - the
            # walk above already produced the right answer.
            log.debug("Unable to record run scans: %s", e)

    def run_attr(self, run, name):
        return self._attr_reader.read(run, name)

    def run_attrs(self, run):
        return self._attr_reader.read_all(run)

    def run_flag(self, run, name):
        return self._flag_reader.read(run, name)

    def run_scalar(self, run, prefix, tag, qual, step):
        return self._scalar_reader.read(run, prefix, tag, qual, step)

    def run_scalars(self, run):
        return list(self._scalar_reader.iter_scalars(run))


def _init_run_index_tables(db):
    db.execute(
        """
      CREATE TABLE IF NOT EXISTS scalar (
        run,
        prefix,
        tag,
        first_val,
        first_step,
        last_val,
        last_step,
        min_val,
        min_step,
        max_val,
        max_step,
        avg_val,
        total,
        count
      )
    """
    )
    db.execute(
        """
      CREATE INDEX IF NOT EXISTS scalar_i
      ON scalar (run, prefix, tag)
    """
    )
    db.execute(
        """
      CREATE TABLE IF NOT EXISTS scalar_source (
        run,
        prefix,
        path_digest
      )
    """
    )
    db.execute(
        """
      CREATE UNIQUE INDEX IF NOT EXISTS scalar_source_pk
      ON scalar_source (run, prefix)
    """
    )
    db.execute(
        """
      CREATE TABLE IF NOT EXISTS run_scan (
        run TEXT PRIMARY KEY,
        status TEXT,
        prefixes TEXT,
        is_batch INTEGER
      )
    """
    )
    scan_cols = {row[1] for row in db.execute("PRAGMA table_info(run_scan)").fetchall()}
    if "is_batch" not in scan_cols:
        db.execute("ALTER TABLE run_scan ADD COLUMN is_batch INTEGER")


def iter_run_scalars(run):
    index = RunIndex()
    index.refresh([run], ["scalar"])
    for s in index.run_scalars(run):
        yield s


def scalars(run, val="last_val", key=None):
    key = key or (lambda s: s["tag"])
    return {key(s): s[val] for s in iter_run_scalars(run)}


def logged_attrs(run):
    return _run_logged_attrs(run)
