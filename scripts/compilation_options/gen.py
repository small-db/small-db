import copy
import itertools
from pprint import pprint
import yaml


def get_options():
    compilation_options_path = "compilation-options.yaml"
    f = open(compilation_options_path, "r")
    v = yaml.safe_load(f)
    return v["compilation_options"]


def gen_cargo_features(options: list[dict]):
    START_LINE = "# ===[COMPILATION OPTIONS START]==="
    END_LINE = "# ===[COMPILATION OPTIONS END]==="

    content = ""
    for option in options:
        # The only key in the dictionary is the name.
        name = list(option.keys())[0]

        # Add comment.
        content += f"# {name}\n"

        sub_options = option[name]
        for sub_option in sub_options:
            content += f"{sub_option} = []\n"
        content += "\n"

    update_content("Cargo.toml", START_LINE, END_LINE, content)


class TestTarget:
    target_name: str
    human_name: str
    featuers_args: str


def get_test_targets(options: list[dict]) -> list[TestTarget]:
    option_list = []
    for option in options:
        name = list(option.keys())[0]
        sub_options = option[name]
        option_list.append(sub_options)

    targets = []
    for mode in itertools.product(*option_list):
        target = TestTarget()
        target.target_name = "test_" + "_".join(mode)
        target.featuers_args = ", ".join(mode)
        target.human_name = f"test ({target.featuers_args})"

        targets.append(target)

    return targets


def gen_make_test(options: list[dict]):
    START_LINE = "# ===[COMPILATION OPTIONS START]==="
    END_LINE = "# ===[COMPILATION OPTIONS END]==="

    test_targets = get_test_targets(options)

    content = ""
    for test_target in test_targets:
        # declare target
        content += f"{test_target.target_name}:\n"

        log_path = f"{test_target.target_name}.log"
        # clear log file
        content += f'\techo "" > {log_path}\n'

        # print mode
        content += f'\techo "Running tests with features: {test_target.featuers_args}" | tee -a {log_path}\n'

        # run tests
        content += f'\tRUST_LOG=info cargo test --features "{test_target.featuers_args}" -- --test-threads=1 2>&1 | tee -a {log_path}\n'

        content += "\n"

    make_test = "test:\n"
    for test_target in test_targets:
        make_test += f"\t{test_target.target_name}\n"

    content = make_test + "\n\n" + content

    update_content("Makefile", START_LINE, END_LINE, content)


def gen_actions(options: list[dict]):
    workflow_path = ".github/workflows/test.yml.bak"
    f = open(workflow_path, "r")
    content = f.read()

    test_targets = get_test_targets(options)
    for test_target in test_targets:
        new_content = copy.deepcopy(content)
        new_content = new_content.replace(
            "name: test", f"name: {test_target.human_name}"
        )
        new_content = new_content.replace(
            "make test", f"make {test_target.target_name}"
        )

        # write the new content to the workflow file
        workflow_path = f".github/workflows/{test_target.target_name}.yml"
        f = open(workflow_path, "w")
        f.write(new_content)


def gen_readme(options: list[dict]):
    test_targets = get_test_targets(options)
    budgets = ""
    for test_target in test_targets:
        budget = f"[![{test_target.human_name}](https://github.com/small-db/small-db/actions/workflows/{test_target.target_name}.yml/badge.svg)](https://github.com/small-db/small-db/actions/workflows/{test_target.target_name}.yml)\n\n"
        budgets += budget

    START_LINE = "<!-- [COMPILATION OPTIONS START] -->"
    END_LINE = "<!-- [COMPILATION OPTIONS END] -->"
    update_content("README.md", START_LINE, END_LINE, budgets)


def update_content(file_path: str, start_line: str, end_line: str, new_content: str):
    f = open(file_path, "r")
    lines = f.readlines()
    f.close()

    in_range = False
    with open(file_path, "w") as f:
        for line in lines:
            if line.strip() == start_line:
                in_range = True
                f.write(start_line + "\n")
                f.write(new_content)
                continue

            if line.strip() == end_line:
                in_range = False
                f.write(end_line + "\n")
                continue

            if not in_range:
                f.write(line)


if __name__ == "__main__":
    options = get_options()

    gen_cargo_features(options)

    gen_make_test(options)

    gen_actions(options)

    gen_readme(options)
