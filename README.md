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
