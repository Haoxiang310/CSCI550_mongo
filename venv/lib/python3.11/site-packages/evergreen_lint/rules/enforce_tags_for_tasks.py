from __future__ import annotations

import re
from typing import List, NamedTuple, Optional, cast

from evergreen_lint.model import LintError, Rule


class TagGroupsConfig(NamedTuple):
    group_name: str
    tag_list: List[str]
    min_num_of_tags: int = 0
    max_num_of_tags: int = 1000
    tag_regex: Optional[str] = None


class EnforceTagsForTasksConfig(NamedTuple):
    tag_groups: List[TagGroupsConfig]

    @classmethod
    def from_config_dict(cls, config_dict) -> EnforceTagsForTasksConfig:
        return cls(
            tag_groups=[
                TagGroupsConfig(**tag_group_config)
                for tag_group_config in config_dict.get("tag_groups")
            ]
        )


class EnforceTagsForTasks(Rule):
    """
    Enforce tags presence in task definitions.

    The configuration example:

    ```
    - rule: "enforce-tags-for-tasks"
      tag_groups:
        - group_name: "tag_group_name"
          # the rule will fail, if tag matches the regex and do not match any tag from `tag_list`
          tag_regex: required_tag_.+
          tag_list:
            - required_tag_test_1
            - required_tag_test_2
            - required_tag_test_3
          # the rule will fail, if the number of tags is less than `min_num_of_tags` or more than `max_num_of_tags`
          min_num_of_tags: 1
          max_num_of_tags: 1
    ```

    """

    @staticmethod
    def name() -> str:
        return "enforce-tags-for-tasks"

    @staticmethod
    def defaults() -> dict:
        return {"tag_groups": []}

    def __call__(self, config: dict, yaml: dict) -> List[LintError]:
        tag_regex_mismatch_error_msg = (
            "Task tags requirement is not met. The task '{task_name}' has tags that matches"
            " '{tag_regex}' tag regex of '{group_name}' tag group and is not listed in the"
            " group: {tag_list}. Please remove the following tag(s) from the task"
            " '{task_name}': {tags_to_remove}."
        )
        num_of_tags_mismatch_error_msg = (
            "Task tags requirement is not met. The task '{task_name}' should have no less"
            " than '{min_num_of_tags}' and no more than '{max_num_of_tags}' tag(s) of"
            " '{group_name}' tag group list: {tag_list}. Found tags: {matching_tags}."
            " Please add/remove tag(s) of the tag group list to/from the task '{task_name}'"
            " to match the requirement."
        )

        failed_checks = []
        rule_config = EnforceTagsForTasksConfig.from_config_dict(config)

        for task_def in cast(List, yaml.get("tasks")):
            actual_tags = task_def.get("tags")
            actual_tags_set = set()
            if actual_tags is not None:
                actual_tags_set = set(actual_tags)

            for tag_group_config in rule_config.tag_groups:
                matching_tag_regex_tags = actual_tags_set
                if tag_group_config.tag_regex is not None:
                    matching_tag_regex_tags = {
                        t for t in actual_tags_set if re.match(tag_group_config.tag_regex, t)
                    }

                matching_tag_list_tags = {
                    t for t in matching_tag_regex_tags if t in tag_group_config.tag_list
                }
                tags_to_remove = matching_tag_regex_tags - matching_tag_list_tags
                num_of_matching_tags = len(matching_tag_list_tags)

                if tags_to_remove:
                    failed_checks.append(
                        tag_regex_mismatch_error_msg.format(
                            task_name=task_def["name"],
                            tag_regex=tag_group_config.tag_regex,
                            group_name=tag_group_config.group_name,
                            tag_list=tag_group_config.tag_list,
                            tags_to_remove=list(tags_to_remove),
                        )
                    )

                if (
                    num_of_matching_tags < tag_group_config.min_num_of_tags
                    or num_of_matching_tags > tag_group_config.max_num_of_tags
                ):
                    failed_checks.append(
                        num_of_tags_mismatch_error_msg.format(
                            task_name=task_def["name"],
                            min_num_of_tags=tag_group_config.min_num_of_tags,
                            max_num_of_tags=tag_group_config.max_num_of_tags,
                            group_name=tag_group_config.group_name,
                            tag_list=tag_group_config.tag_list,
                            matching_tags=list(matching_tag_list_tags),
                        )
                    )

        return failed_checks
