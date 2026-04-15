# Guild AI (SQLite-indexed fork)

A fork of [Guild AI](https://guildai.org) that adds a SQLite index over the
run store to eliminate redundant filesystem scans.

## Motivation

Upstream Guild AI resolves every command by walking the run directory tree
and re-reading each run's metadata from disk. No results are cached between
invocations, so runtime scales linearly with the number of runs — including
for operations that should be O(1), such as `guild run --start ID`, where
the run still has to be located by scan.

The penalty is most severe on networked filesystems, where each stat/open
pays round-trip latency. This fork was built primarily to make Guild AI
usable on compute clusters backed by network storage, but it also yields
measurable speedups on local disks.

## What this fork changes

- Introduces a SQLite-backed run index that caches the fields needed for
  run lookup and the common status filters (`-Sc`, `-Se`, `-Sp`, `-Ss`).
- Read paths (lookup, listing, filtering) are served from the index instead
  of re-scanning the run directories.
- Write paths are designed to match upstream cost in the common case, and
  are faster when filtering is involved.

## Worker-mode: `GUILD_NO_INDEX_WRITES`

On heavily-parallel clusters (many concurrent `guild run` processes sharing
one index over NFS) the index's filelock can starve under load, which
historically showed up as silent "ghost" runs (a run completes on disk but
its status is never written to the index).

Set `GUILD_NO_INDEX_WRITES=1` in the environment of a `guild run` invocation
(e.g. from a slurm sbatch template) to make that process skip all index
writes. Instead of writing, it `touch`es a per-run dirty marker at
`<index-db-path>.dirty.d/<run_id>`. The worker never acquires the index lock.

The next `guild` operation on any host that sees the same index (typically
`guild runs list`, `guild compare`, `guild view`, …) lists the per-run
marker directory and runs a **delta sync**: only the marked runs are
re-read from disk and upserted into the index, then their markers are
cleared. Result: workers run without lock contention, and the cost of the
reconciling read scales with the number of *changed* runs, not the total
number of runs in the store.

A global marker at `<index-db-path>.dirty` is still honored as an escape
hatch: if it is fresher than the DB file, the next read runs a full
`index_sync` instead of a delta sync. Workers no longer touch the global
marker themselves; it is only set by admin intervention or by legacy code
paths.

Behavioral details:

- Workers with `GUILD_NO_INDEX_WRITES=1` never trigger the dirty-sync on
  their own reads — only "headnode" (unset env var) invocations do the
  resync. This avoids thundering-herd syncs when many workers start at
  once.
- Per-run marker freshness is tracked by `mtime`, compared against the DB
  file's mtime. Clearing is compare-and-delete: if a worker re-touches a
  marker while a resync is in progress, the marker survives the clear and
  the next operation syncs again.
- A full sync also clears per-run markers whose `mtime` is older than the
  sync started, so a full resync followed by a delta read doesn't
  redundantly re-upsert runs the full sync already covered.
- Workers can still *read* the index as a cache. Misses fall through to
  the lock-free filesystem scan in `find_runs`, so `guild run --restart
  <id>` resolves correctly even when the worker's view of the index is
  stale.
- Headnode write paths (e.g. `guild runs delete`, `guild label`) are
  unchanged; they continue to take the lock and update the index directly.
  A write-lock timeout now raises loudly instead of being silently
  swallowed, so failures are visible at process exit.

Both delta and full resyncs upsert affected runs from disk (not just
inserts), so a run that was registered on the headnode as `pending` and
then finalized on a worker lands with its terminal status on the next
headnode read. During the sync, cached index rows are bypassed so `Run`
properties are recomputed from the filesystem.

In-flight runs (no `exit_status` yet, no `STAGED`/`PENDING`/`LOCK.remote`
marker) are skipped by the resync: on another NFS host their status would
degrade to `error` via a local-PID check. The per-run marker for an
in-flight run is left in place so the next sync retries after the worker
writes `exit_status`.

---


Guild AI is an [open source](LICENSE.txt) toolkit that automates and
optimizes machine learning experiments.

- Run unmodified training scripts, capturing each run result as a unique
  experiment
- Automate trials using grid search, random search, and Bayesian
  optimization
- Compare and analyze runs to understand and improve models
- Backup training related operations such as data preparation and test
- Archive runs to S3 or other remote systems
- Run operations remotely on cloud accelerators
- Package and distribute models for easy reproducibility

For more on features, see [Guild AI - Features](https://guildai.org).

Important links:

- **[Get Started with Guild AI](https://guildai.org/start)**
- **[Get Help with Guild AI](https://guildai.org)**
- **[Latest Release on PyPI](https://pypi.python.org/pypi/guildai)**
- **[Documentation](https://guildai.org/docs/)**
- **[Issues on GitHub](https://github.com/guildai/guildai/issues)**
