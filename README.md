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
- Consolidates a run's init-time attributes into a single
  `.guild/attrs.json` ("attr blob") instead of one file per attribute,
  cutting per-run filesystem writes (see below). This is the **default**.
- Parses run attribute files with the C YAML loader plus an integer
  fast-path, so the bulk attribute reads during an index sync are several
  times faster than the pure-Python `yaml.safe_load` they replaced.
- Reduces per-stage index I/O: the dirty markers a worker writes are
  batched to one per run, and the post-stage status print no longer opens
  the index DB.
- Runs the index DB with `synchronous=OFF`. The index is a derived cache
  (rebuilt on corruption and kept fresh by dirty markers), so per-commit
  fsyncs buy nothing and are removed — the dominant per-stage cost on a
  networked index DB.
- Memoizes the per-script flag-import cache in-process, so staging many
  trials of the same operation validates the cache once rather than
  re-`stat`-ing it for every trial.
- Caches the pkg_resources `WorkingSet` per path in `EntryPointResources`, so
  resolving the cwd model no longer rescans every `sys.path` entry on each
  call. Model resolution wraps the lookup in a temporary `SetPath` and restores
  the full `sys.path` on exit; that restore previously rebuilt the WorkingSet
  (re-reading every installed distribution's metadata) on every `guild run` —
  ~218 `find_on_path` scans per op resolution, the second-biggest per-stage
  cost on a networked install. Now an unchanged path reuses its cached set.
- Avoids recomputing the VCS commit per staged run: recording `vcs_commit`
  shells out to `git` (incl. a `git status` working-tree walk, costly on a
  networked filesystem). The parallel stager computes it once and passes it to
  every trial via `GUILD_VCS_COMMIT`; `write_vcs_commit` uses that value
  instead of re-running git. (`NO_VCS_COMMIT=1` still skips it entirely.)
- Ships the cluster staging/running tools (`guild-parallel-stager`,
  `guild-slurm-runner`) in-tree under `guild.cluster` (see below).

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

## Rebuilding the index

`guild check --rebuild-index` deletes the index DB and rebuilds it from
scratch by scanning every run directory. Unlike a normal sync (which reuses
the existing DB file), this drops the DB entirely, so it also clears a stale
schema or leftover rows from an older version of the index format. Dirty and
sync markers are cleared too, leaving a freshly-synced index.

## Consolidated run attrs: `.guild/attrs.json`

Upstream writes each run attribute as its own file under `.guild/attrs/`
(`cmd`, `flags`, `host`, `id`, `op`, ...). On a networked filesystem each
file is a separate round-trip, so a single staged run costs ~16 small
writes just for its metadata. This fork batches the init-time attributes
and flushes them as **one** `.guild/attrs.json` file, cutting a staged
run's run-directory writes from ~20 to ~8.

- **On by default.** Set `GUILD_ATTRS_BLOB=0` (or `false`/`no`) to opt out
  and write the legacy per-attribute files instead.
- **Read path is always blob-aware**, in this order: pending in-memory
  write buffer → per-attribute file → `attrs.json`. A per-attribute file
  takes precedence over the blob so a later `write_attr` can override a
  value copied in via the blob (e.g. batch trials copy the proto's
  `attrs.json`, then write their resolved flags per-file). Old per-file
  runs and new blob runs both read correctly, and a full or delta index
  sync handles a mix of the two.
- **Only immutable init attrs are blobbed.** Attributes written after init
  — `started`, `deps`, `env`, `exit_status`, `stopped` — stay per-file, so
  the blob doesn't change after a run starts and a restart reflushes it
  cleanly (init attrs that aren't re-written, such as `id`/`initialized`,
  are preserved by merging into the existing blob).
- **Tools that read attr files by path fall back to the blob.** `guild cat
  -p .guild/attrs/<name>` emits the value from `attrs.json` when the
  per-file form is absent, and `guild diff --attrs/--flags/--env/--deps`
  materializes attributes from the blob into a temp dir for the diff.
- The blob is cached per `Run` object and invalidated on the file's
  `(mtime, size)`, so a long-lived object still sees external rewrites
  (e.g. a worker finalizing a run) — matching the always-fresh semantics
  of per-attribute files.

## Cluster staging tools (`guild.cluster`)

Two console scripts for staging and running large batches on a cluster are
vendored in-tree (from [guild-utils](https://github.com/jkbjh/guild-utils))
so they ship and version with this fork:

- **`guild-parallel-stager`** — expands a flag-list spec into trials and
  stages them. It stages **in-process** (reusing one imported `guild` per
  worker) instead of forking a fresh `guild` process per trial, which
  avoids re-paying Python import + charset-detection startup on every
  trial; for large batches this is the dominant local cost. It stages in
  **worker mode automatically** — it sets `GUILD_NO_INDEX_WRITES=1` for its
  workers so per-trial index writes become per-run dirty markers, then
  resyncs the index once after all trials are staged.
- **`guild-slurm-runner`** — selects staged runs (by filter, ids, or a
  JSON file) and either executes them directly (`--exec`) or submits them to
  SLURM (`--sbatch`). Three submission shapes: the default writes one
  independent sbatch job per chunk; `--job-array` packs all chunks into a
  single SLURM job array (one job id, fixed chunk per task); and
  `--shared-queue` submits an array of pull-workers that dynamically claim
  runs from a shared, filesystem-backed queue (atomic `mkdir` claim markers,
  tagged with the array generation for crash-safe **resume by resubmit**),
  giving dynamic load balancing when run times vary. The shared queue lives
  under `$GUILD_HOME/_slurm_queue` by default (override with `--queue-dir`);
  set `--use-jobs N` to your max concurrency. With `--job-array`, add
  `--shuffle` to randomize run order before chunking so long/short runs spread
  evenly across tasks rather than clumping; if the task count exceeds the
  cluster's `MaxArraySize` (auto-detected) it auto-splits across several arrays,
  and `--num-arrays N` forces a specific count. `--max-running N` caps how many
  tasks run at once overall (split across the arrays via SLURM's `%` throttle).
- **`guild-stage-diagnose`** — measures the actual wall-clock cost of the
  operations staging performs, to locate the bottleneck on a given filesystem
  (esp. a cluster NAS): latency of filesystem primitives (stat, create+write,
  fsync, mkdir, rename, unlink, readdir), SQLite commit latency at
  `synchronous=OFF/NORMAL/FULL`, end-to-end staging phase timings (with
  `--operation`), with `--operation --profile` a cProfile of a single warm
  stage showing which functions/syscalls dominate (built-ins like
  `posix.stat`/`open` reveal NFS-blocking time), and with
  `--operation --compare-exec` a comparison of staging N trials sequentially
  in-process vs via joblib/loky (n_jobs=1 and N) — including distinct worker
  PIDs — to isolate parallel-layer overhead from the actual per-trial work. It
  only writes to its own temp dirs and never touches real runs. Run
  `guild-stage-diagnose --help`, or `python -m guild.cluster.stage_diagnose`.

The scripts are registered as entry points, so a normal install of this
fork provides them; `import guild.cluster.parallel_stager` /
`guild.cluster.guild_runner` expose the same functions for embedding.

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
