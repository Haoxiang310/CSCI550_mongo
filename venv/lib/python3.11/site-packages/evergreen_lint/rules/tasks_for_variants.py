from typing import Dict, List, Optional, Set, cast

from evergreen_lint.model import LintError, Rule

TasksSet = Set[str]
TaskVariantMapping = List[Dict[str, List[str]]]


class TasksForVariantsConfig:
    def __init__(self, raw_mappings: TaskVariantMapping, failed_checks: List[LintError]):
        self._variant_mappings: Dict[str, TasksSet] = {}
        self.unused_variants: Set[str] = set()

        for mapping in raw_mappings:
            tasks = set(mapping["tasks"])
            for variant in mapping["variants"]:
                # Each variant can only be defined with one set of tasks.
                if variant in self._variant_mappings:
                    failed_checks.append(
                        f"Invalid linter config: '{variant}' appeared more than once"
                    )
                self._variant_mappings[variant] = tasks
                self.unused_variants.add(variant)

    def tasks_for_variant(self, variant: str) -> Optional[TasksSet]:

        res = self._variant_mappings.get(variant, None)
        if res is not None:
            self.unused_variants.remove(variant)
        return res


class TasksForVariants(Rule):

    """
    Enforce task definitions for variants.

    The configuration will look like:

    ```
    - rule: "tasks-for-variants"
      task-variant-mappings:
        - name: release-variants-tasks
          tasks:
          - task1
          - task2
          variants:
          - variant1
          - variant2
        - name: asan-variant-tasks
          tasks:
          - task1
          - task2
          - task3
          variants:
          - variant3
          - variant4
    ```

    """

    @staticmethod
    def name() -> str:
        return "tasks-for-variants"

    @staticmethod
    def defaults() -> dict:
        return {"task-variant-mappings": {}}

    @staticmethod
    def _get_task_set_from_list(task_list):
        return {task["name"] for task in task_list}

    def __call__(self, config: dict, yaml: dict) -> List[LintError]:
        error_msg = (
            "Mismatched task list for variant '{variant}'. Expected '{expected}', got '{actual}'"
        )

        failed_checks: List[str] = []

        config_wrapper = TasksForVariantsConfig(
            cast(TaskVariantMapping, config.get("task-variant-mappings")), failed_checks
        )

        variants = yaml.get("buildvariants", [])
        if variants is None:
            failed_checks.append("No variants defined in Evergreen config")

        for variant_obj in variants:
            variant = variant_obj["name"]
            expected_tasks = config_wrapper.tasks_for_variant(variant)
            actual_tasks = self._get_task_set_from_list(variant_obj["tasks"])

            if expected_tasks is None:
                continue

            if expected_tasks != actual_tasks:
                failed_checks.append(
                    error_msg.format(variant=variant, expected=expected_tasks, actual=actual_tasks)
                )

        if config_wrapper.unused_variants:
            failed_checks.append(
                f"Invalid linter config: unknown variant names: {config_wrapper.unused_variants}"
            )

        return failed_checks
