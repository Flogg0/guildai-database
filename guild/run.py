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

import contextlib
import json
import os
import random
import re
import threading
import time

import uuid
import yaml

from guild import opref as opreflib
from guild import util
from guild import yaml_util

# Attr files are read on every run lookup and, in bulk, on every index sync.
# The pure-Python yaml.safe_load dominates sync time, so use the C loader when
# libyaml is available and a strict fast-path for plain base-10 integers
# (timestamps, exit_status) that the YAML int resolver decodes identically.
try:
    _AttrYamlLoader = yaml.CSafeLoader
except AttributeError:  # libyaml not built
    _AttrYamlLoader = yaml.SafeLoader

# Subset of PyYAML's int resolver where Python int() yields the same value:
# no leading zeros (avoids YAML octal), no '+', no underscores.
_PLAIN_INT_RE = re.compile(r"^-?(?:0|[1-9][0-9]*)$")


def _load_attr(text):
    """Parse a run attr value, equivalent to yaml.safe_load(text).

    Fast-paths plain base-10 integers (the overwhelmingly common attr type)
    and otherwise defers to the C YAML loader.
    """
    if _PLAIN_INT_RE.match(text.strip()):
        return int(text)
    return yaml.load(text, Loader=_AttrYamlLoader)

CORE_RUN_ATTRS = {
    "cmd",
    "deps",
    "env",
    "exit_status",
    "flags",
    "host",
    "id",
    "initialized",
    "label",
    "op",
    "opdef_attrs",
    "platform",
    "plugins",
    "random_seed",
    "run_params",
    "sourcecode_digest",
    "started",
    "stopped",
    "user",
    "user_flags",
    "vcs_commit",

    # TODO: The following are core but are associated with
    # plugins. These should be defined by the plugin but there is
    # currently no such interface.
    "pip_freeze",
    "r_sys_info",
    "r_packages_loaded",
}

LEGACY_RUN_ATTRS = {
    "resolved_deps",
    "opdef",
}


# Parsed .guild/attrs.json keyed by (path, (mtime, size)). Bounded because a
# command that touches many runs would otherwise retain every blob it read.
_ATTRS_BLOB_CACHE_MAX = 256
_attrs_blob_cache = {}


def _cache_attrs_blob(key, blob):
    if len(_attrs_blob_cache) >= _ATTRS_BLOB_CACHE_MAX:
        _attrs_blob_cache.clear()
    _attrs_blob_cache[key] = blob


def _drop_cached_attrs_blob(path):
    for key in [k for k in _attrs_blob_cache if k[0] == path]:
        del _attrs_blob_cache[key]


class Run:
    __properties__ = [
        "id",
        "path",
        "short_id",
        "opref",
        "pid",
        "status",
        "timestamp",
    ]

    def __init__(self, id, path):
        self.id = id
        self.path = path
        self._guild_dir = os.path.join(self.path, ".guild")
        self._opref = None
        self._index_row = None
        # Attr-blob prototype: _attr_buffer holds {name: encoded} while attr
        # writes are batched (during run init); _attrs_blob caches a parsed
        # .guild/attrs.json. Both None until used. See write_attr / __getitem__.
        self._attr_buffer = None
        self._attrs_blob = None
        self._attrs_blob_mtime = None
        self._attrs_blob_pinned = False
        self._props = util.PropertyCache(
            [
                ("timestamp", None, self._get_timestamp, 1.0),
                ("pid", None, self._get_pid, 1.0),
            ]
        )

    def _ensure_index_row(self):
        if self._index_row is not None:
            return self._index_row
        from guild import var
        # During a dirty-triggered sync, _do_index_sync rebuilds rows from
        # disk. Returning cached (stale) row values here would feed those
        # stale values back into the sync upsert. Bypass so Run properties
        # compute from the filesystem.
        if getattr(var._index_local, 'in_dirty_sync', False):
            self._index_row = {}
            return self._index_row
        try:
            import sqlite3
            conn = var._get_index_conn()
            row = conn.execute(
                f"SELECT {var.INDEX_ROW_SQL} FROM runs WHERE run_id = ?",
                (self.id,),
            ).fetchone()
            if row:
                self._index_row = var.index_row_from_cols(row)
                return self._index_row
        except sqlite3.DatabaseError:
            var._nuke_index()
        except Exception:
            pass
        self._index_row = {}
        return self._index_row

    @property
    def short_id(self):
        return self.id[:8]

    @property
    def index_sync_stamp(self):
        """Stamp of the last time this run's index row was written from disk.

        None when the run has no row, or the row predates the stamp. Derived
        caches key on it: unchanged stamp means the dirty-marker protocol has
        not seen a write to this run, so what the index holds is current.
        """
        return self._ensure_index_row().get("synced_at")

    @property
    def indexed_op_name(self):
        """The formatted operation name recorded in the run index, if any.

        None when the run has no index row (the caller then formats it from
        the run itself).
        """
        return self._ensure_index_row().get("op_name")

    @property
    def dir(self):
        """Alias for path attr."""
        return self.path

    @property
    def opref(self):
        if not self._opref:
            encoded = self._read_opref()
            if encoded:
                try:
                    self._opref = opreflib.OpRef.parse(encoded)
                except opreflib.OpRefError as e:
                    raise opreflib.OpRefError(
                        f"invalid opref for run {self.id!r} ({self.path}): {e}"
                    )
        return self._opref

    def _read_opref(self):
        row = self._ensure_index_row()
        cached = row.get("opref")
        if cached:
            return cached
        return util.try_read(self._opref_path())

    def _opref_path(self):
        return self.guild_path("opref")

    def write_opref(self, opref):
        self.write_encoded_opref(str(opref))
        # Cache the opref we just wrote so a subsequent run.opref read (e.g.
        # the post-stage "staged as ..." print) doesn't open the index DB to
        # look up a row that, for a just-created run, cannot exist yet. Saves
        # an index open+probe per staged run on networked storage.
        self._opref = opref

    def write_encoded_opref(self, encoded):
        with open(self._opref_path(), "w") as f:
            f.write(encoded)
        self._opref = None

    def reset_opref(self):
        self._opref = None

    @property
    def pid(self):
        return self._props.get("pid")

    def _get_pid(self):
        lockfile = self.guild_path("LOCK")
        try:
            raw = open(lockfile, "r").read(10)
        except (IOError, ValueError):
            return None
        else:
            try:
                return int(raw)
            except ValueError:
                return None

    @property
    def status(self):
        row = self._ensure_index_row()
        cached = row.get("status")
        if cached:
            return cached
        if os.path.exists(self.guild_path("LOCK.remote")):
            return "running"
        if os.path.exists(self.guild_path("PENDING")):
            return "pending"
        if os.path.exists(self.guild_path("STAGED")):
            return "staged"
        return self._local_status()

    @property
    def remote(self):
        remote_lock_path = self.guild_path("LOCK.remote")
        return util.try_read(remote_lock_path, apply=str.strip)

    @property
    def timestamp(self):
        return self._props.get("timestamp")

    def _get_timestamp(self):
        return util.find_apply(
            [
                lambda: self.get("started"),
                lambda: self.get("initialized"),
                lambda: None,
            ]
        )

    @property
    def batch_proto(self):
        proto_dir = self.guild_path("proto")
        proto_opref_path = os.path.join(proto_dir, ".guild", "opref")
        if os.path.exists(proto_opref_path):
            return for_dir(proto_dir)
        return None

    def _local_status(self):
        exit_status = self.get("exit_status")
        if exit_status is not None:
            return _status_for_exit_status(exit_status)
        local_pid = self._get_pid()
        if local_pid is not None and util.pid_exists(local_pid):
            return "running"
        return "error"

    def get(self, name, default=None):
        try:
            val = self[name]
        except KeyError:
            return default
        else:
            return val if val is not None else default

    def attr_names(self):
        names = set(util.safe_listdir(self._attrs_dir()))
        names.update(self._load_attrs_blob())
        if self._attr_buffer:
            names.update(self._attr_buffer)
        return sorted(names)

    def has_attr(self, name):
        if self._attr_buffer is not None and name in self._attr_buffer:
            return True
        if name in self._load_attrs_blob():
            return True
        return os.path.exists(self._attr_path(name))

    def iter_attrs(self):
        for name in self.attr_names():
            try:
                yield name, self[name]
            except KeyError:
                pass

    _INDEX_READABLE = frozenset(
        ("flags", "tags", "label", "started", "stopped", "initialized")
    )

    # Attrs written once at run init and never mutated. When the index has a
    # row for the run its value is definitive, so an absent attr is answered
    # without touching the run dir at all -- unlike _INDEX_READABLE, where a
    # missing value falls through to the filesystem. A NULL column means the
    # row predates the attr being indexed, which does fall through.
    _INDEX_INIT_ATTRS = frozenset(("sourcecode_digest", "opdef_attrs", "compare"))

    def __getitem__(self, name):
        if name in self._INDEX_READABLE:
            row = self._ensure_index_row()
            val = row.get(name)
            if val is not None:
                return val
            # A NULL column is not evidence the attr is absent: a row can be
            # written before the attr exists (index_register_run runs before
            # `started` is written), so a miss must still be resolved against
            # the run dir. Only _INDEX_INIT_ATTRS below can answer "absent"
            # from the index, because those record it affirmatively.
        elif name in self._INDEX_INIT_ATTRS and self._attr_buffer is None:
            indexed = self._ensure_index_row().get(name)
            if indexed is not None:
                if indexed == "":
                    raise KeyError(name)
                if name == "sourcecode_digest":
                    return indexed
                try:
                    return json.loads(indexed)
                except ValueError:
                    pass
        # Read order: pending write buffer, then a per-attr file, then the
        # consolidated attr blob. A per-attr file wins over the blob so a later
        # write_attr() can override a value brought in via the blob -- e.g.
        # batch trials copytree the proto's attrs.json, then write resolved
        # flags/label per-file. All three store the same yaml-encoded text.
        buf = self._attr_buffer
        if buf is not None and name in buf:
            return _load_attr(buf[name])
        try:
            f = open(self._attr_path(name), "r")
        except IOError:
            pass
        else:
            with f:
                return _load_attr(f.read())
        blob = self._load_attrs_blob()
        if name in blob:
            return _load_attr(blob[name])
        raise KeyError(name)

    def _attr_path(self, name):
        return os.path.join(self._attrs_dir(), name)

    def _attrs_dir(self):
        return os.path.join(self._guild_dir, "attrs")

    def _attrs_blob_path(self):
        return os.path.join(self._guild_dir, "attrs.json")

    def _attrs_blob_stamp(self):
        """(mtime, size) of the attr blob, or None if it cannot be read."""
        try:
            st = os.stat(self._attrs_blob_path())
        except OSError:
            return None
        return (st.st_mtime, st.st_size)

    def _load_attrs_blob(self):
        """Return the parsed {name: encoded} attr blob, or {} if none.

        Cached on the Run but invalidated when the file's (mtime, size) change,
        so a long-lived Run object still sees the blob rewritten by another
        process (e.g. a restart) -- matching the always-fresh semantics of
        per-attr files. Size is checked alongside mtime because mtime
        resolution on some networked filesystems is coarse (1-2s), and a
        same-second rewrite usually changes the file size. The blob stores the
        same yaml-encoded text as the per-attr files, so values decode
        identically via _load_attr.
        """
        if self._attrs_blob_pinned and self._attrs_blob is not None:
            return self._attrs_blob
        path = self._attrs_blob_path()
        try:
            st = os.stat(path)
            stamp = (st.st_mtime, st.st_size)
        except OSError:
            self._attrs_blob = {}
            self._attrs_blob_mtime = None
            return self._attrs_blob
        if self._attrs_blob is not None and self._attrs_blob_mtime == stamp:
            return self._attrs_blob
        # A single command can hold several Run objects for the same run --
        # restaging builds three, since it re-resolves the run by id through
        # the command layer -- and each would otherwise open and parse the
        # same unchanged file for itself. The cache key includes the stamp, so
        # a rewritten blob is a miss by construction and the staleness window
        # is the one described above, not a wider one.
        key = (path, stamp)
        cached = _attrs_blob_cache.get(key)
        if cached is not None:
            self._attrs_blob = cached
            self._attrs_blob_mtime = stamp
            return cached
        try:
            with open(path) as f:
                self._attrs_blob = json.load(f)
            self._attrs_blob_mtime = stamp
            _cache_attrs_blob(key, self._attrs_blob)
        except (IOError, OSError, ValueError):
            self._attrs_blob = {}
            self._attrs_blob_mtime = None
        return self._attrs_blob

    @contextlib.contextmanager
    def pinned_attrs(self):
        """Serves attr reads from a single blob load for the duration.

        Reading several attrs from one run normally re-stats attrs.json before
        each one, so a long-lived Run still sees a rewrite by another process.
        A caller taking a point-in-time view of a run (an index refresh reads
        a handful of attrs back to back) gains nothing from revalidating
        between those reads and pays a round-trip for each.
        """
        prev = self._attrs_blob_pinned
        self._attrs_blob_pinned = True
        try:
            yield self
        finally:
            self._attrs_blob_pinned = prev

    def attr_blob_encoded(self, name):
        """Raw yaml-encoded text for `name` from the consolidated blob, or
        None if absent. For tools (cat, diff) that read attr files by path and
        need to fall back to the blob when the per-attr file isn't present.
        """
        return self._load_attrs_blob().get(name)

    def attrs_blob(self):
        """The full {name: encoded} consolidated attr blob ({} if none)."""
        return dict(self._load_attrs_blob())

    def begin_attr_buffer(self):
        """Start batching attr writes in memory for the consolidated blob.

        On by default; set GUILD_ATTRS_BLOB=0 to opt out and write legacy
        per-attr files instead.
        """
        if os.getenv("GUILD_ATTRS_BLOB", "1") in ("0", "false", "False", "no"):
            return
        if self._attr_buffer is None:
            self._attr_buffer = {}

    def flush_attr_buffer(self):
        """Write buffered attrs as one .guild/attrs.json and clear the buffer.

        No-op when no buffer is active.
        """
        buf = self._attr_buffer
        self._attr_buffer = None
        if not buf:
            return
        # Merge into any existing blob rather than replacing it. On a restart
        # the run is re-initialized in place but init only re-writes a subset
        # of attrs (init_skel skips id/initialized since they already exist,
        # and environment attrs like host aren't re-set), so a plain overwrite
        # would drop the attrs that weren't re-buffered.
        merged = {}
        try:
            with open(self._attrs_blob_path()) as f:
                merged = json.load(f)
        except (IOError, OSError, ValueError):
            merged = {}
        merged.update(buf)
        with open(self._attrs_blob_path(), "w") as f:
            json.dump(merged, f)
        # Keep what was just written instead of forcing the next attr read to
        # re-open and re-parse it. The stamp comes from the file, so a rewrite
        # by another process still invalidates as usual.
        self._attrs_blob = merged
        self._attrs_blob_mtime = self._attrs_blob_stamp()

    def get_opdef_attr(self, name, default=None):
        return (self.get("opdef_attrs") or {}).get(name, default)

    def __repr__(self):
        return f"<{self.__class__.__module__}.{self.__class__.__name__} '{self.id}'>"

    def init_skel(self):
        util.ensure_dir(self.guild_path("attrs"))
        # `.guild` now exists (ensure_dir created attrs under it); let later
        # marker writes skip their redundant ensure_dir on this run object.
        self._guild_dir_ensured = True
        if not self.has_attr("initialized"):
            self.write_attr("id", self.id)
            self.write_attr("initialized", timestamp())
        else:
            # The run dir is being reused (restart/restage). The previous
            # cycle's terminal attrs describe an execution that is over:
            # leaving them pairs a fresh `started` with a stale `stopped`,
            # which reads back as a negative duration. `initialized` is
            # already stat'd above, so this costs nothing for a new run.
            self.del_attr("stopped")
            self.del_attr("exit_status")

    def guild_path(self, *subpath):
        if subpath is None:
            return self._guild_dir
        return os.path.join(*((self._guild_dir,) + tuple(subpath)))

    _INDEX_ATTRS = frozenset(
        (
            "flags", "tags", "label", "started", "stopped", "initialized",
            "sourcecode_digest", "opdef_attrs", "compare",
        )
    )

    def write_attr(self, name, val, raw=False):
        if not raw:
            encoded = yaml_util.encode_yaml(val, strict=True)
        else:
            encoded = val
        if self._attr_buffer is not None:
            # Batched: defer to one consolidated write at flush_attr_buffer().
            self._attr_buffer[name] = encoded
        else:
            with open(self._attr_path(name), "w") as f:
                f.write(encoded)
                f.write(os.linesep)
                f.close()
        # attrs.json is untouched by either branch here - the buffered write
        # lands in flush_attr_buffer, and a per-attr file takes precedence
        # over the blob in __getitem__ without changing it. Dropping the
        # parsed blob would make the next attr read re-open and re-parse a
        # file that has not changed.
        if name in self._INDEX_ATTRS:
            indexed_val = val if not raw else encoded
            # Keep the cached index row in step with the write. Without this a
            # later self[name] serves the pre-write value from the cache (the
            # row is only re-read when _index_row is None), which read back
            # into an index write would clobber the value just written.
            if self._index_row is not None:
                self._index_row[name] = indexed_val
            try:
                from guild import var
                var.index_update_attr(self, name, indexed_val)
            except Exception:
                pass

    def del_attr(self, name):
        try:
            os.remove(self._attr_path(name))
        except OSError:
            pass
        # Also drop it from a pending write buffer and the consolidated blob,
        # otherwise the deleted attr would still read back from attrs.json.
        if self._attr_buffer is not None:
            self._attr_buffer.pop(name, None)
        # Copy before mutating: the parse may be shared with other Run
        # objects for this run via _attrs_blob_cache.
        blob = dict(self._load_attrs_blob())
        if name in blob:
            del blob[name]
            _drop_cached_attrs_blob(self._attrs_blob_path())
            if blob:
                with open(self._attrs_blob_path(), "w") as f:
                    json.dump(blob, f)
            else:
                try:
                    os.remove(self._attrs_blob_path())
                except OSError:
                    pass
            self._attrs_blob = None
            self._attrs_blob_mtime = None
        # Clear the indexed column too, mirroring write_attr -- otherwise a
        # deleted index attr (e.g. `guild label --clear`) would still read back
        # the stale value from the SQLite index.
        if name in self._INDEX_ATTRS:
            # Mirror write_attr: a NULL in the cached row means "unknown", so
            # the next read falls through to the run dir and sees the deletion.
            if self._index_row is not None:
                self._index_row[name] = None
            try:
                from guild import var
                var.index_update_attr(self, name, None)
            except Exception:
                pass

    def iter_files(self, all_files=False, follow_links=False):
        for root, dirs, files in os.walk(self.path, followlinks=follow_links):
            if not all_files and root == self.path:
                try:
                    dirs.remove(".guild")
                except ValueError:
                    pass
            for name in dirs:
                yield os.path.join(root, name)
            for name in files:
                yield os.path.join(root, name)

    def iter_guild_files(self, subpath):
        guild_path = self.guild_path(subpath)
        if os.path.exists(guild_path):
            for root, dirs, files in os.walk(guild_path):
                rel_root = os.path.relpath(root, guild_path)
                if rel_root == ".":
                    rel_root = ""
                for name in dirs:
                    yield os.path.join(rel_root, name)
                for name in files:
                    yield os.path.join(rel_root, name)


def _status_for_exit_status(exit_status):
    assert exit_status is not None, exit_status
    if exit_status == 0:
        return "completed"
    if exit_status < 0:
        return "terminated"
    return "error"


__last_ts = None
__last_ts_lock = threading.Lock()


def timestamp():
    """Returns an integer use for run timestamps.

    Ensures that subsequent calls return increasing values.
    """
    ts = time.time_ns() // 1000
    with __last_ts_lock:
        if __last_ts is not None and __last_ts >= ts:
            ts = __last_ts + 1
        globals()["__last_ts"] = ts
    return ts


def timestamp_seconds(ts):
    """Returns seconds float from value generated by `timestamp`."""
    return float(ts / 1000000)


def mkid():
    return uuid.uuid4().hex


def for_dir(run_dir, id=None):
    if not id:
        id = os.path.basename(run_dir)
    return Run(id, run_dir)


def random_seed():
    return random.randint(0, pow(2, 32))
