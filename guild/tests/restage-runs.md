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

    >>> run("guild runs restage -F 'operation = hello' -y")
    Restaged 2 run(s)

The two `hello` runs are staged. `hello-file` is still completed.

Note that staging sets a run's start time, so the restaged runs sort
ahead of `hello-file`.

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

    >>> hola = run_capture("guild select 1")

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
