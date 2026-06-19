"""Count a user's outstanding SLURM jobs via ``squeue`` text output.

``squeue`` only lists active work (pending + running), so the number of lines
it reports for a user is the number of jobs still left to finish -- with one
wrinkle: job arrays. Running array tasks are listed one per line, but *pending*
array tasks are collapsed into a single line whose JOBID carries a range, e.g.
``1234_[5-100]`` (optionally throttled, ``1234_[5-100%4]``, or a comma list,
``1234_[5,7,9-12]``). We expand those ranges so each array task counts once.
"""

import argparse
import os
import re
import subprocess
import sys


def count_array_tasks(jobid):
    """Return the number of individual tasks a squeue JOBID field represents.

    Plain jobs (``1234``) and single array tasks (``1234_5``) count as 1;
    a collapsed pending range (``1234_[5-100%4]``, ``1234_[5,7,9-12]``)
    expands to the number of tasks in its spec.
    """
    if "_[" not in jobid:
        return 1
    spec = jobid.split("_[", 1)[1].rstrip("]")
    spec = spec.split("%", 1)[0]  # drop the "%N" concurrency throttle
    total = 0
    for part in spec.split(","):
        m = re.fullmatch(r"(\d+)-(\d+)", part)
        if m:
            lo, hi = int(m.group(1)), int(m.group(2))
            total += hi - lo + 1
        else:
            total += 1
    return total


def squeue_lines(user):
    """Yield ``(jobid, state)`` tuples for the user's active jobs."""
    try:
        out = subprocess.check_output(
            ["squeue", "-u", user, "-h", "-o", "%i %t"],
            text=True,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        sys.exit("error: squeue not found on PATH")
    except subprocess.CalledProcessError as e:
        sys.exit(f"error: squeue failed: {e.stderr.strip() or e}")
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        jobid, state = line.split()
        yield jobid, state


def count_jobs(user, running_only=False):
    return sum(
        count_array_tasks(jobid)
        for jobid, state in squeue_lines(user)
        if not running_only or state == "R"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Count a user's outstanding SLURM jobs (job-array aware) via squeue.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "user",
        nargs="?",
        help="SLURM user whose jobs to count (defaults to the $USER env variable)",
    )
    parser.add_argument(
        "--running",
        "-r",
        action="store_true",
        help="count only jobs currently running instead of all jobs left to finish",
    )
    args = parser.parse_args()
    user = args.user or os.environ.get("USER")
    if not user:
        parser.error("no user given and $USER is not set")
    print(count_jobs(user, running_only=args.running))


if __name__ == "__main__":
    main()
