# Restage runs

The `runs restage` command resets runs to `staged` status so they can
be started again with `guild run --start`. Unlike `--proto`, a
restaged run keeps its ID and run directory.

We use the `hello` sample project.

    >>> use_project("hello")

Generate two `hello` runs and one `hello-file` run.

    >>> run("guild run hello msg=hola -y")
    hola

    >>> run("guild run hello msg=bonjour -y")
    bonjour

    >>> run("guild run hello-file -y")
    Reading message from hello.txt
    Hello, from a file!
    <BLANKLINE>
    Saving message to msg.out

Each run is completed.

    >>> run("guild runs -s")
    [1]  hello-file  completed  file=hello.txt
    [2]  hello       completed  msg=bonjour
    [3]  hello       completed  msg=hola

## Restaging with a filter

Runs are selected for restaging using the standard run filters. Here
we restage the `hello` runs, leaving `hello-file` alone.

    >>> run("guild runs restage -F 'operation = hello' -y -j 1")
    Restaged 2 run(s)

The two `hello` runs are staged. `hello-file` is still completed.

Note that staging sets a run's start time, so the restaged runs sort
ahead of `hello-file`. `-j 1` restages them serially, in selection
order, which is what makes the order below deterministic - the parallel
default leaves their order relative to *each other* undefined.

    >>> run("guild runs -s")
    [1]  hello       staged     msg=hola
    [2]  hello       staged     msg=bonjour
    [3]  hello-file  completed  file=hello.txt

Runs are restaged in place - no runs are created or deleted.

    >>> ids = run_capture("guild select -A").split()

    >>> len(ids)
    3

## Starting a restaged run

A staged run is started with `guild run --start`. Flags are preserved
from the original run.

    >>> hola = run_capture("guild select -F 'msg = hola'")

    >>> run(f"guild run --start {hola} -y")
    hola

    >>> run("guild runs -s")
    [1]  hello       completed  msg=hola
    [2]  hello       staged     msg=bonjour
    [3]  hello-file  completed  file=hello.txt

The run kept its ID - the restart reused the original run directory.

    >>> sorted(run_capture("guild select -A").split()) == sorted(ids)
    True

## Previewing runs to restage

Without `--yes`, the runs to restage are previewed for confirmation.

    >>> run("guild runs restage -F 'operation = hello'")
    You are about to restage the following runs:
      [...]  hello  ...  completed  msg=hola
      [...]  hello  ...  staged     msg=bonjour
    Restage 2 run(s)? (y/N)
    <exit 1>

Nothing is restaged when the prompt is declined.

    >>> run("guild runs -s")
    [1]  hello       completed  msg=hola
    [2]  hello       staged     msg=bonjour
    [3]  hello-file  completed  file=hello.txt

## Nothing to restage

    >>> run("guild runs restage -Fo not-an-op -y")
    Nothing to restage.

## Run timestamps across a restage

Restaging stamps a fresh start time, but the run has not run yet, so the
previous cycle's stop time and exit status are cleared. Left in place,
they would pair a new start time with an old stop time and read back as
a negative duration.

    >>> from guild import run_util
    >>> from guild import var

    >>> bonjour = run_capture("guild select -F 'msg = bonjour'")

    >>> staged = var.get_run(bonjour)

    >>> staged.status
    'staged'

    >>> staged.get("stopped") is None
    True

    >>> staged.get("exit_status") is None
    True

    >>> run_util.run_duration(staged) is None
    True

Starting the run stamps the time it actually ran, not the time it was
staged.

    >>> staged_at = staged.get("started")

    >>> run(f"guild run --start {bonjour} -y")
    bonjour

    >>> restarted = var.get_run(bonjour)

    >>> restarted.status
    'completed'

    >>> restarted.get("started") > staged_at
    True

The index and the run dir are read by different code paths, so they are
checked against each other - a stale index start time inflates a run's
duration by however long it sat staged.

    >>> attr_path = path(
    ...     guild_home(), "runs", bonjour, ".guild", "attrs", "started")

    >>> with open(attr_path) as f:
    ...     on_disk = int(f.read())

    >>> restarted.get("started") == on_disk
    True

    >>> restarted.get("stopped") >= on_disk
    True
