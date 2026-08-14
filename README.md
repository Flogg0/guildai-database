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
- Reading runs for display (`guild compare`, `guild runs list`, …) does not
  walk run directories at all. The index records where each run's event
  files live and whether they hold logged attrs, so a run untouched since
  its last sync is answered entirely from SQLite. Previously every refresh
  walked each run's tree twice — once to find logged attrs, once to find
  scalars — which dominated the cost of any query over many runs. See
  [Read path](#read-path) below.
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

## Read path

A query that displays runs (`guild compare --csv`, `guild runs list`, the
API and view backends) reads only the two SQLite databases in the common
case. Over a store of ~25k runs on a local disk this took `guild compare
--csv` from ~130 filesystem operations per run to ~0.2 for completed runs;
on networked storage, where each of those was a round-trip, that is the
difference between tens of minutes and seconds.

What made the difference:

- **One query per result set, not per run.** `index_query_runs` selects the
  columns backing `Run._index_row` and prefills them, instead of each `Run`
  re-querying the same columns for itself on first property access. The
  scalar cache is loaded the same way — one query for the whole set rather
  than one per run per column.
- **Event dirs come from the index.** A `run_scan` table records, per run,
  which subdirs hold event files, whether any hold logged attrs, and the
  event filenames. The staleness digest is then recomputed by `stat`-ing
  those known files rather than listing the directory to rediscover them.
- **Invalidation is the dirty-marker protocol, nothing else.** Every write
  of a run's row from disk stamps `synced_at`; the scan record stores that
  stamp and treats a change as its only invalidation signal. A run the
  markers have not flagged is served from the index; a flagged run is
  re-walked and re-read. There is no third state in which a reader
  re-derives freshness for itself.
- **Init-time attrs are indexed.** `sourcecode_digest`, `opdef_attrs` and
  `compare` are written once when a run is initialized and never mutated, so
  they are columns. For these, an empty string records "this run has no such
  attr" — a definitive answer that avoids opening anything. This is *not*
  generalized to other attrs: a NULL column elsewhere means "not captured"
  (a row can be written before an attr exists), and a miss still falls back
  to the run dir.
- **Non-terminal status is never served from the query snapshot.** Only
  `completed`, `error` and `terminated` are; anything in flight is resolved
  from the run's own marker files, because a snapshot taken when the query
  ran can otherwise report a run as `pending` after it has failed.

Trade-off: a new event *subdirectory* appearing in a run whose row has not
been re-synced is not noticed. Changes to existing event files are, via the
digest. Anything that goes through the normal lifecycle — a run finishing, a
restart — re-syncs the row and re-scans the run.

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
- A worker's *read* can still owe the index a write: `index_query_runs` and
  `iter_run_dirs` prune rows for runs whose directory is gone. With writes
  disabled that prune is deferred as one marker per pruned run, not the
  global marker — a delta sync already deletes the row for a run whose dir
  has no opref, so the deferred delete is expressed exactly. Escalating
  instead would let a single worker read force a re-read of every run on
  disk: measured over a synthetic store with one stale row, the resync took
  385ms and re-read 3999 runs at 4000 total (linear in the total), versus
  ~1ms and no run reads via the delta path (flat in the total).
- Per-run marker freshness is tracked by `mtime`, compared against the
  `dirty_synced_mtime` recorded for *that run* — the mtime of the marker it
  was last re-read for. Comparing against the index DB file's mtime (as this
  once did) aliases every run together: a write about one run retires a
  pending marker for another, after which the marker is never fresh, never
  processed and never cleared, and its run's row stays stale until a full
  resync. The recorded value is a marker mtime rather than a local clock
  reading so that both sides come from the filesystem holding the markers,
  and a clock offset between a worker and the headnode cannot silently
  retire a pending marker. Clearing is compare-and-delete: if a worker
  re-touches a marker while a resync is in progress, the marker survives the
  clear and the next operation syncs again.
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

### Parallelizing the resync read phase: `GUILD_RESYNC_WORKERS`

After a large staging batch the reconciling resync becomes the dominant
cost: it reopens the index and reads every dirty run from disk to build its
index row (opref + attrs). On a networked filesystem each run is a handful
of serial metadata round-trips (~14 `stat` + ~7 `open` per run, measured),
and the sync walks them one run at a time, so it is **latency-bound**.

Set `GUILD_RESYNC_WORKERS=N` (e.g. `16`) to fan the *read* phase out across
`N` threads. The blocking filesystem calls release the GIL, so the per-run
round-trips overlap; the SQLite writes stay serial in the parent (the
connection is single-threaded), so the index result is byte-for-byte
identical to a serial sync. Each worker thread takes the dirty-sync
filesystem-fallback path, so a worker read never re-enters the index DB.

- **Off by default (serial).** Threading only wins when reads are
  filesystem-latency bound. On a *local* disk the reads are CPU bound
  (opref/YAML parsing under the GIL) and extra threads only add contention
  — measurably *slower*. So this is an opt-in for cluster/NAS staging; set
  it in the staging job's environment, not globally.
- Modeling NAS latency (~37 ms of round-trips per run), 600 runs resynced
  in **22.6 s serial → 1.5 s with 16 threads** (~15×), with identical index
  output. Speedup is near-linear up to ~16 threads, then tapers.
- Only engages for batches of ≥64 dirty runs; smaller (interactive) syncs
  stay serial regardless, since thread setup wouldn't pay off.

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

## Restaging runs: `guild runs restage`

Resets finished runs to `staged` so they can be started again — useful for
re-queueing a batch to `guild-slurm-runner`, which selects staged runs.

    guild runs restage -Fo train -Sc          # restage completed 'train' runs
    guild runs restage -Se -y                 # restage runs that errored

Runs are selected with the standard run filters (as with `guild runs
delete`); with no `RUN` and no filters, all runs are selected.

Runs with a live process are **never restaged** — restarting a run under an
active process would corrupt it — and a warning names how many were skipped
for this reason. Note this can't be decided from a run's status: the index
caches `pending` for the life of a run (`running` is never written to it, by
design), so a running run reports `pending`. The check uses the run's `LOCK`
file instead.

Each run is restaged **in place** — it keeps its ID, run directory,
operation, and flags, and its dependencies are re-resolved. Files written by
a previous start are *not* removed; use `guild run --proto RUN` instead to
start from a clean run directory. Note that staging sets a run's start time,
so restaged runs sort to the top of `guild runs`. Under the parallel default
(below) their order relative to *each other* is not defined; `-j 1` restages
in selection order.

If a run can't be restaged (e.g. it's missing its op configuration), it is
reported as a warning and the rest of the batch still restages; the command
then exits non-zero.

Restaging is not a status flip: each run gets a full staging cycle, so a
large restage is latency-bound on networked storage (~95 filesystem
operations per run). Runs are therefore restaged **in parallel by default**,
one job per CPU but never more workers than there are runs, reusing the same
machinery as `guild-parallel-stager`: each worker invokes Guild's entrypoint
in-process (skipping interpreter startup per run) with per-run index writes
disabled, and the dirty markers they leave are folded in by a single delta
sync at the end.

    guild runs restage -Se -y                 # parallel, up to one job per CPU
    guild runs restage -Se -y -j 8            # cap at 8 workers
    guild runs restage -Se -y -j 1            # serial, in this process

Use `-j 1` when you want everything in one process. That path defers the
whole batch's index writes to **a single commit at the end**, rather than one
commit per run as a standalone stage would do. Either way, runs that failed
to restage are simply absent from the index update, so a partial failure
still leaves the index consistent with what's on disk.

Parallelism only pays when each stage is latency-bound. On a warm local page
cache a stage is a few milliseconds and worker startup costs more than it
saves — `-j 1` is faster there.

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
  `--time` sets a SLURM wall-clock limit (e.g. `24:00:00` or `1-00:00:00`,
  validated against the accepted formats) by injecting `#SBATCH --time` into the
  sbatch template; omit it to use the partition default.
  `--limit N` processes at most N runs per invocation, so a run set larger than
  the cluster's ~10000 job/array-task ceiling can be drained in batches: re-run
  the same command and already running/finished runs drop out of the filter, so
  each invocation picks up the next batch. With `--guildfilter` the limit is
  pushed into the guild query (SQL `LIMIT`) so only N runs are loaded (the most
  recent N matching the filter); with `--runids`/`--runsfile` the first N are
  taken.
- **`guild-slurm-jobcount`** — prints how many of a user's SLURM jobs are
  still left to finish, parsing `squeue` text output (`guild-slurm-jobcount
  [USER]`; the user defaults to `$USER`). It is **job-array aware**: running array tasks are listed one per
  line, but pending tasks are collapsed by `squeue` into ranges
  (`1234_[5-100]`, `1234_[5-100%4]`, `1234_[5,7,9-12]`), so it expands those
  ranges to count each task once. Add `--running`/`-r` to count only the jobs
  currently running instead of everything still queued.
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
