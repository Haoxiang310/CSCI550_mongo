"""entry point for evglint."""
import os
import sys
from typing import Dict, List, Optional

import click

from evergreen_lint.config import STUB, load_config
from evergreen_lint.model import LintError
from evergreen_lint.rules import RULES
from evergreen_lint.yamlhandler import load_file


@click.group()
@click.option("-c", "--config", type=click.Path(exists=True))
@click.pass_context
def main(ctx: click.Context, config: Optional[os.PathLike]) -> None:
    ctx.ensure_object(dict)
    ctx.obj["config"] = None
    if config is not None:
        click.echo(f"Load config file: {config}")
        ctx.obj["config"] = load_config(config)


@main.command()
@click.pass_context
def lint(ctx: click.Context) -> None:
    """Lint an Evergreen YAML file."""
    if ctx.obj["config"] is None:
        click.echo("-c/--config: a config file is required")
        sys.exit(1)
    ret = 0
    rules = RULES
    configs = {}
    for rulename, rulecls in rules.items():
        configs[rulename] = rulecls().defaults()

    # override the defaults with the config file
    rules = {}
    filenames = ctx.obj["config"]["files"]
    for rule in ctx.obj["config"]["rules"]:
        rules[rule["rule"]] = RULES[rule["rule"]]
        configs[rule["rule"]] = {**configs[rule["rule"]], **rule}
        del configs[rule["rule"]]["rule"]

    for yaml_file in filenames:
        yaml_dict = load_file(yaml_file)
        errors: Dict[str, List[LintError]] = {}
        for rulename, rulecls in rules.items():
            instance = rulecls()
            if rulename in configs:
                config = configs[rulename]
            else:
                config = instance.defaults()
            rule_errors = instance(config, yaml_dict)
            if rule_errors:
                errors[rulename] = rule_errors

        err_count = 0
        for error_list in errors.values():
            err_count += len(error_list)

        if not err_count:
            print(f"0 errors found in '{yaml_file}'")
        if err_count:
            errors_plural = "error"
            if err_count > 1:
                errors_plural = "errors"
            print(f"{err_count} {errors_plural} found in '{yaml_file}':")
            print(
                "For help resolving errors, see the helpful documentation at "
                f"{ctx.obj['config']['help_url']}"
            )
            print_nl = False
            for rule, error_list in errors.items():
                for error in error_list:
                    if print_nl:
                        print("")
                    print(f"{rule}:", error)
                    print_nl = True
            ret = 1

    sys.exit(ret)


@main.command()
def stub() -> None:
    """Generate a stub configuration."""
    print(STUB)
    sys.exit(0)


if __name__ == "__main__":
    main(obj={})
