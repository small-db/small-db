import itertools
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


def gen_make_test(options: list[dict]):
    START_LINE = "# ===[COMPILATION OPTIONS START]==="
    END_LINE = "# ===[COMPILATION OPTIONS END]==="

    modes = []

    for option in options:
        name = list(option.keys())[0]
        sub_options = option[name]
        modes.append(sub_options)

    content = ""
    test_targets = []
    for mode in itertools.product(*modes):
        test_target = "test_" + "_".join(mode)
        test_targets.append(test_target)
        # declare target
        content += f"{test_target}:\n"

        log_path = f"{test_target}.log"
        # clear log file
        content += f'\techo "" > {log_path}\n'

        # print mode
        content += f'\techo "Running tests with features: {mode}" | tee -a {log_path}\n'

        # run tests
        mode_str = ", ".join(mode)
        content += f'\tRUST_LOG=info cargo test --features "{mode_str}" -- --test-threads=1 2>&1 | tee -a {log_path}\n'

        content += "\n"

    make_test = "test:\n"
    for test_target in test_targets:
        make_test += f"\t{test_target}\n"

    content = make_test + "\n\n" + content

    update_content("Makefile", START_LINE, END_LINE, content)


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
