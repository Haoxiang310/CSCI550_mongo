"""Command line driver for evergreen API."""
from __future__ import absolute_import

import json
from enum import Enum
from itertools import islice
from typing import Optional

import click
import yaml

from evergreen import EvergreenApi
from evergreen.resource_type_permissions import PermissionableResourceType, RemovablePermission

DATE_FORMAT = "%Y-%m-%d"

DisplayFormat = Enum("DisplayFormat", "human json yaml")


def fmt_output(fmt, data):
    """
    Convert the given data into the specified format.

    :param fmt: DisplayFormat to use.
    :param data: Data to convertn.
    :return: Data is specified format.
    """
    if fmt == DisplayFormat.json:
        return json.dumps(data, indent=4)
    if fmt == DisplayFormat.yaml:
        return yaml.safe_dump(data)
    return data


@click.group()
@click.option(
    "--json", "display_format", flag_value=DisplayFormat.json, help="Write output in json."
)
@click.option(
    "--yaml", "display_format", flag_value=DisplayFormat.yaml, help="Write output in yaml."
)
@click.option(
    "--human-readable",
    "display_format",
    flag_value=DisplayFormat.human,
    default=True,
    help="Write output in a human readable format.",
)
@click.pass_context
def cli(ctx, display_format):
    """Create common CLI options."""
    ctx.ensure_object(dict)
    ctx.obj["api"] = EvergreenApi.get_api(use_config_file=True)
    ctx.obj["format"] = display_format


@cli.command()
@click.pass_context
def list_hosts(ctx):
    """List the hosts running in evergreen."""
    api = ctx.obj["api"]
    fmt = ctx.obj["format"]
    host_list = api.all_hosts()
    click.echo(fmt_output(fmt, [host.json for host in host_list]))


@cli.command()
@click.pass_context
@click.option("-p", "--project", required=True)
@click.option("-l", "--limit", type=int)
def list_patches(ctx, project, limit):
    """Get the patches for the given project."""
    api = ctx.obj["api"]
    fmt = ctx.obj["format"]
    patches = []
    for i, p in enumerate(api.patches_by_project(project)):
        if limit and i >= limit:
            break
        patches.append(p.json)
    click.echo(fmt_output(fmt, patches))


@cli.command()
@click.pass_context
def list_projects(ctx):
    """List the evergreen projects."""
    api = ctx.obj["api"]
    fmt = ctx.obj["format"]
    project_list = api.all_projects()
    projects = [project.json for project in project_list]
    click.echo(fmt_output(fmt, projects))


@cli.command()
@click.pass_context
@click.option("--project", required=True)
@click.option("--start", type=int)
@click.option("--limit", type=int)
@click.option("--revision_start", type=int)
@click.option("--revision_end", type=int)
def list_versions(
    ctx,
    project: str,
    start: Optional[int],
    limit: Optional[int],
    revision_start: Optional[int],
    revision_end: Optional[int],
) -> None:
    """Get the versions for the given project."""
    api = ctx.obj["api"]
    fmt = ctx.obj["format"]
    version_list = api.versions_by_project(
        project, start=start, limit=limit, revision_start=revision_start, revision_end=revision_end
    )
    versions_to_display = [version.json for version in islice(version_list, None, limit)]

    click.echo(fmt_output(fmt, versions_to_display))


@cli.command()
@click.pass_context
@click.option("--target", required=True)
@click.option("--msg", required=True)
def send_slack_message(ctx, target: str, msg: str) -> None:
    """Send a Slack message to the specified target."""
    api = ctx.obj["api"]
    api.send_slack_message(target, msg)


@cli.command()
@click.pass_context
@click.option(
    "-a",
    "--after-date",
    required=True,
    type=click.DateTime(formats=[DATE_FORMAT]),
    help="The earliest date to use 'YYYY-MM-DD'.",
)
@click.option(
    "-b",
    "--before-date",
    required=True,
    type=click.DateTime(formats=[DATE_FORMAT]),
    help="The latest date to use 'YYYY-MM-DD'.",
)
@click.option("-p", "--project", required=True)
@click.option("-d", "--distros", multiple=True)
@click.option("--group-by")
@click.option("-g", "--group-num-days")
@click.option("-r", "--requesters", multiple=True)
@click.option("-s", "--sort")
@click.option("--tests", multiple=True)
@click.option("-t", "--tasks", multiple=True)
@click.option("-v", "--variants", multiple=True)
def test_stats(
    ctx,
    after_date,
    before_date,
    project,
    distros,
    group_by,
    group_num_days,
    requesters,
    sort,
    tests,
    tasks,
    variants,
):
    """Get the test stats specified."""
    api = ctx.obj["api"]
    fmt = ctx.obj["format"]

    test_stat_list = api.test_stats_by_project(
        project,
        after_date,
        before_date,
        group_num_days,
        requesters,
        tests,
        tasks,
        variants,
        distros,
        group_by,
        sort,
    )
    test_statistics = [t.json for t in test_stat_list]
    click.echo(fmt_output(fmt, test_statistics))


@cli.command()
@click.pass_context
@click.option(
    "-a",
    "--after-date",
    required=True,
    type=click.DateTime(formats=[DATE_FORMAT]),
    help="The earliest date to use 'YYYY-MM-DD'.",
)
@click.option(
    "-b",
    "--before-date",
    required=True,
    type=click.DateTime(formats=[DATE_FORMAT]),
    help="The latest date to use 'YYYY-MM-DD'.",
)
@click.option("-p", "--project", required=True)
@click.option("-d", "--distros", multiple=True)
@click.option("--group-by")
@click.option("-g", "--group-num-days")
@click.option("-r", "--requesters", multiple=True)
@click.option("-s", "--sort")
@click.option("-t", "--tasks", multiple=True)
@click.option("-v", "--variants", multiple=True)
def task_stats(
    ctx,
    after_date,
    before_date,
    project,
    distros,
    group_by,
    group_num_days,
    requesters,
    sort,
    tasks,
    variants,
):
    """Get the specified task stats."""
    api = ctx.obj["api"]
    fmt = ctx.obj["format"]

    task_stat_list = api.task_stats_by_project(
        project,
        after_date,
        before_date,
        group_num_days,
        requesters,
        tasks,
        variants,
        distros,
        group_by,
        sort,
    )
    task_statistics = [t.json for t in task_stat_list]
    click.echo(fmt_output(fmt, task_statistics))


RELIABILITY_GROUP_MAPPING = {
    "task": "task",
    "variant": "task_variant",
    "distro": "task_variant_distro",
}


@cli.command()
@click.pass_context
@click.option(
    "-a",
    "--after-date",
    required=True,
    type=click.DateTime(formats=[DATE_FORMAT]),
    help="The earliest date to use 'YYYY-MM-DD'.",
)
@click.option(
    "-b",
    "--before-date",
    required=True,
    type=click.DateTime(formats=[DATE_FORMAT]),
    help="The latest date to use 'YYYY-MM-DD'.",
)
@click.option(
    "-p", "--project", required=True, help="The evergreen project, eg 'mongodb-mongo-master'"
)
@click.option("-d", "--distros", multiple=True, help="The list of distributions.")
@click.option(
    "--group-by",
    type=click.Choice(RELIABILITY_GROUP_MAPPING.keys()),
    default="task",
    help="Group the results by 'task', 'variant' or 'distro'. Defaults to 'task'",
)
@click.option(
    "-g",
    "--group-num-days",
    default=28,
    help="The number of days to group results by. Defaults to 28.",
)
@click.option("-r", "--requesters", multiple=True, help="The requesters.")
@click.option("-s", "--sort", help="The sort order, can be earliest or latest.")
@click.option(
    "-t",
    "--tasks",
    multiple=True,
    required=True,
    help="The list of tasks, e.g. 'lint' , 'compile'. Required, no default.",
)
@click.option("-v", "--variants", multiple=True, help="The list of build variants.")
def task_reliability(
    ctx,
    after_date,
    before_date,
    project,
    distros,
    group_by,
    group_num_days,
    requesters,
    sort,
    tasks,
    variants,
):
    """
    Get the Task Reliability scores for the matching tasks.

    \b
    Examples:
    \b
        # Get the scores for mongodb-mongo-master project, compile task (grouped by task)
        # for today (1 day, grouped by days 1).
        $> evg-api --json task-reliability -p mongodb-mongo-master -t compile
    OR
        $> evg-api --json task-reliability -p mongodb-mongo-master -t compile \\
            --group-by task

    \b
        # Get the scores for mongodb-mongo-master project, compile task (grouped by variant)
        # for today (1 day, grouped by days 1).
        $> evg-api --json task-reliability -p mongodb-mongo-master -t compile \\
            --group-by variant

    \b
        # Get the scores for mongodb-mongo-master project, compile and lint tasks (grouped by distro)
        # for today (1 day, grouped by days 1).
        $> evg-api --json task-reliability -p mongodb-mongo-master -t compile -t lint \\
            --group-by distro
    \b
        # Get the scores for mongodb-mongo-master project, lint and compile tasks (grouped by distro)
        # for the last 28 days.
        $> evg-api --json task-reliability -p mongodb-mongo-master -t lint -t compile -g 1 \\
            -a $(date -I --date="27 days ago")
    OR
        $> evg-api --json task-reliability -p mongodb-mongo-master -t lint -t compile -g 1 \\
            -a $(date -I --date="27 days ago") -b $(date -I)
    OR
        $> evg-api --json task-reliability -p mongodb-mongo-master -t lint -t compile -g 1 \\
            --group_num_days 28

    \b
        # Get the scores for mongodb-mongo-master project, lint and compile tasks (grouped by distro)
        # for each day for the last 28 days.
        $> evg-api --json task-reliability -p mongodb-mongo-master -t lint -t compile \\
            --group_by distro -a $(date -I --date="27 days ago")

    \b
        # Get the scores for mongodb-mongo-master project, lint task (grouped by task) , grouped in
        # batches of 28 days for all dates after 168 days ago.
        $> evg-api   --json task-reliability   -p mongodb-mongo-master -t lint  --group-by task \\
            -g 28 -a $(date -I --date="$((28 * 6 - 1)) days ago")

    \f
    :see: 'task reliability
        <https://github.com/evergreen-ci/evergreen/wiki/REST-V2-Usage#taskreliability>'
    """
    api = ctx.obj["api"]
    fmt = ctx.obj["format"]

    task_reliability_list = api.task_reliability_by_project(
        project,
        after_date,
        before_date,
        group_num_days,
        requesters,
        tasks,
        variants,
        distros,
        RELIABILITY_GROUP_MAPPING[group_by],
        sort,
    )
    task_reliability_scores = [t.json for t in task_reliability_list]
    click.echo(fmt_output(fmt, task_reliability_scores))


@cli.command()
@click.pass_context
@click.option("-v", "--version", "version_id", required=True)
@click.option("--builds", is_flag=True, default=False, help="Include builds of version in output")
def version_stats(ctx, version_id, builds):
    """
    Collect stats for the given evergreen version.

    :param ctx: Command context.
    :param version_id: Id of version to analyze.
    :param builds: Include builds of version in output.
    """
    api = ctx.obj["api"]
    fmt = ctx.obj["format"]

    version = api.version_by_id(version_id)
    if fmt == DisplayFormat.human:
        click.echo(version.get_metrics())
    else:
        click.echo(fmt_output(fmt, version.get_metrics().as_dict(include_children=builds)))


@cli.command()
@click.pass_context
@click.option("-b", "--build", "build_id", required=True)
@click.option("--tasks", is_flag=True, default=False, help="Include tasks of build in output")
def build_stats(ctx, build_id, tasks):
    """
    Collect stats for the given evergreen build.

    :param ctx: Command context.
    :param build_id: Id of build to analyze.
    :param tasks: If true include tasks in output.
    """
    api = ctx.obj["api"]
    fmt = ctx.obj["format"]

    build = api.build_by_id(build_id)
    if fmt == DisplayFormat.human:
        click.echo(build.get_metrics())
    else:
        click.echo(fmt_output(fmt, build.get_metrics().as_dict(include_children=tasks)))


@cli.command()
@click.pass_context
@click.option("--project", required=True, help="The project name")
@click.option("--commit", required=True, help="The full 40-char commit hash")
def manifest(ctx, project, commit):
    """
    Get a manifest for the given project and commit.

    Example: use jq to get a module version associated with a main-repo version:

    evg-api --json manifest --project <PROJECT> --commit <MAIN-REPO-HASH> \\
        | jq --raw-output .modules.<MODULE-NAME>.revision
    """
    api = ctx.obj["api"]
    fmt = ctx.obj["format"]

    manifest = api.manifest(project, commit)
    click.echo(fmt_output(fmt, manifest.json))


@cli.command()
@click.pass_context
@click.option("--user-id", required=False, help="User whose permissions to fetch")
def user_permissions(ctx, user_id):
    """
    Get the evergreen permissions for a given user.

    Gets permissions for the current user if --user-id is not explicitly
    specified.
    """
    api = ctx.obj["api"]
    fmt = ctx.obj["format"]

    if not user_id:
        user_id = api._auth.username
    permissions = api.permissions_for_user(user_id)
    click.echo(fmt_output(fmt, [p.json for p in permissions]))


@cli.command()
@click.pass_context
@click.option("--user-id", required=True, help="User whose permissions to remove")
@click.option(
    "--resource-type",
    required=True,
    type=click.Choice(["project", "distro", "superuser", "all"], case_sensitive=False),
    help="Type of resource for which to delete permissions.",
)
@click.option(
    "--resource-id",
    required=False,
    help="Id of the resource for which to delete permissions. Required unless deleting all permissions.",
)
def delete_user_permissions(ctx, user_id, resource_type, resource_id):
    """Delete all permissions of a given type for a user."""
    api = ctx.obj["api"]
    api.delete_user_permissions(user_id, RemovablePermission(resource_type), resource_id)
    if resource_id:
        click.echo(
            f"Sucessfully deleted {resource_type} permissions for user {user_id} on resource id {resource_id}"
        )
    else:
        click.echo(f"Sucessfully deleted {resource_type} permissions for user {user_id}")


@cli.command()
@click.pass_context
@click.option("--user-id", required=True, help="User to grant roles to.")
@click.option(
    "--role", required=True, multiple=True, help="Role to grant the user.",
)
def give_roles_to_user(ctx, user_id, role):
    """Grant roles to a user."""
    api = ctx.obj["api"]
    api.give_roles_to_user(user_id, list(role))
    click.echo(f"Successfully granted roles {role} to user {user_id}")


@cli.command()
@click.pass_context
@click.option(
    "--role", required=True, help="Role to fetch users for.",
)
def get_users_for_role(ctx, role):
    """Get users having an evergreen role."""
    api = ctx.obj["api"]
    users = api.get_users_for_role(role)
    click.echo(users.users)


@cli.command()
@click.pass_context
@click.option("--resource-id", required=True, help="Resource id to fetch user permissions for.")
@click.option(
    "--resource-type",
    required=True,
    type=click.Choice(["project", "distro", "superuser"], case_sensitive=False),
    help="Type of resource.",
)
def all_user_permissions_for_resource(ctx, resource_id, resource_type):
    """Get all user permissions to a resource."""
    api = ctx.obj["api"]
    user_permissions = api.all_user_permissions_for_resource(
        resource_id, PermissionableResourceType(resource_type)
    )
    click.echo(user_permissions)


@cli.command()
@click.pass_context
@click.option("--patch-id", required=True, help="Patch id to request diff for.")
def patch_diff(ctx, patch_id):
    """Get patch diff for a given patch."""
    api = ctx.obj["api"]
    diff = api.get_patch_diff(patch_id)
    click.echo(diff)


@cli.command()
@click.pass_context
@click.option("--diff-file", required=True, help="The path to the diff file.")
@click.option("--description", required=True, help="The description of the build.")
@click.option("--param", required=True, help="The params to pass to the build.")
@click.option("--base", required=True, help="The base commit of the build.")
@click.option("--project", required=True, help="The project of the build.")
@click.option("--tasks", required=True, help="The tasks to execute.")
@click.option("--variants", required=True, help="The variants to build against.")
@click.option("--author", required=False, default=None, help="Indicate the author of the patch.")
def patch_from_diff(ctx, diff_file, description, param, base, project, tasks, variants, author):
    """Start a patch build based on the diff."""
    api = ctx.obj["api"]
    response = api.patch_from_diff(
        diff_file, param, base, tasks, project, description, variants, author
    )
    click.echo(response)


def main():
    """Create command line application."""
    return cli(obj={})


if __name__ == "__main__":
    main()
