"""Cluster helpers for staging and running Guild operations at scale.

Vendored from https://github.com/jkbjh/guild-utils and adapted to ship with
this fork. Exposes two console scripts (see guildai.dist-info/entry_points.txt):

- ``guild-parallel-stager`` (parallel_stager): expand a flag-list spec into
  trials and stage them. Stages in-process so each persistent worker imports
  guild and warms charset detection once instead of forking a fresh ``guild``
  process per trial.
- ``guild-slurm-runner`` (guild_runner): select staged runs and execute them,
  either directly or by submitting SLURM batch jobs.
"""
