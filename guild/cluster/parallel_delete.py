"""guild-parallel-delete: delete or purge runs with parallel jobs.

usage: guild-parallel-delete [--purge] [-j N] [guild runs delete/purge args]

Runs `guild runs delete` (or, with --purge, `guild runs purge`) with the
given arguments - runs, filters, --permanent, --yes - and `-j N` parallel
jobs, one per CPU unless given. Run dirs are removed on that many threads:
every file in a run is its own delete, a round-trip apiece on a networked
filesystem, which parallel jobs overlap. A soft delete (no --permanent) only
renames each run into trash and gains nothing from jobs.
"""

import os
import sys


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    cmd = "delete"
    if "--purge" in argv:
        argv.remove("--purge")
        cmd = "purge"
    if not _has_jobs_arg(argv):
        argv[:0] = ["--jobs", str(os.cpu_count() or 1)]
    from guild.commands.main import main as guild_cli

    try:
        guild_cli.main(args=["runs", cmd] + argv, standalone_mode=False)
    except SystemExit as e:
        return e.code
    return 0


def _has_jobs_arg(argv):
    return any(
        arg in ("-j", "--jobs") or arg.startswith("--jobs=")
        or (arg.startswith("-j") and arg[2:].isdigit())
        for arg in argv
    )


if __name__ == "__main__":
    sys.exit(main())
