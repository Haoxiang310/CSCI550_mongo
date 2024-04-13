from typing import List, cast

from evergreen_lint.helpers import determine_dependencies_of_task_def
from evergreen_lint.model import LintError, Rule


class DependencyForFunc(Rule):

    """
    Define dependencies that are required if a function is used.

    The configuration will look like:

    ```
    - rule: "dependency-for-func"
      dependencies:
        func_name: [dependency_0, depdendency_1]
    ```

    """

    @staticmethod
    def name() -> str:
        return "dependency-for-func"

    @staticmethod
    def defaults() -> dict:
        return {"dependencies": {}}

    def __call__(self, config: dict, yaml: dict) -> List[LintError]:
        error_msg = (
            "Missing dependency. The task '{task_name}' expects '{dependency}' to be "
            "listed as a dependency due to the use of the '{function}' func."
        )
        failed_checks = []
        dependency_map = config.get("dependencies", {})
        for task_def in cast(List, yaml.get("tasks")):
            actual_dependencies = determine_dependencies_of_task_def(task_def)
            funcs = [cmd["func"] for cmd in task_def.get("commands", []) if "func" in cmd]
            for func in funcs:
                expected_dependencies = dependency_map.get(func, [])
                unmet_dependenices = [
                    dep for dep in expected_dependencies if dep not in actual_dependencies
                ]
                failed_checks.extend(
                    [
                        error_msg.format(task_name=task_def["name"], dependency=dep, function=func)
                        for dep in unmet_dependenices
                    ]
                )

        return failed_checks
