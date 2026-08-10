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

import click

from guild import click_util

from . import runs_support


def restage_params(fn):
    click_util.append_params(
        fn,
        [
            runs_support.runs_arg,
            runs_support.all_filters,
            click.Option(
                ("-y", "--yes"),
                help="Do not prompt before restaging runs.",
                is_flag=True,
            ),
            click.Option(
                ("-j", "--jobs"),
                metavar="N",
                type=click.IntRange(min=1),
                default=1,
                help=(
                    "Restage N runs in parallel (default 1). Each run is a "
                    "full staging cycle, so a large restage is latency-bound "
                    "on networked storage; workers skip per-run index writes "
                    "and the index is resynced once at the end."
                ),
            ),
        ],
    )
    return fn


@click.command("restage")
@restage_params
@click.pass_context
@click_util.use_args
@click_util.render_doc
def restage_runs(ctx, args):
    """Restage runs.

    Restaging resets a run to `staged` status so it can be started
    again with ``guild run --start``. Each run keeps its ID, run
    directory, operation, and flags - Guild re-resolves the run
    dependencies and re-writes its environment in place.

    Files written by a previous start are not removed. Use `--proto`
    with the run command to start from a clean run directory instead.

    Runs that are still running are not restaged - restarting a run
    under an active process would corrupt it. A warning is printed when
    runs are skipped for this reason.

    {{ runs_support.runs_arg }}

    If `RUN` isn't specified, all runs matching the specified filters
    are restaged.

    {{ runs_support.all_filters }}

    """
    from . import runs_impl

    runs_impl.restage(args, ctx)
