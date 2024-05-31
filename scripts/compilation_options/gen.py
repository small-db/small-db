import itertools
import yaml

START_LINE = "# ===[COMPILATION OPTIONS START]==="
END_LINE = "# ===[COMPILATION OPTIONS END]==="


def get_options():
    compilation_options_path = "compilation-options.yaml"
    f = open(compilation_options_path, "r")
    v = yaml.safe_load(f)
    return v["compilation_options"]


def gen_cargo_features(options: list[dict]):
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

    print(content)

    update_content("Cargo.toml", content)

    pass


def gen_make_test(modes):
    # Generate all possible combinations of modes.
    content = "test:\n"
    content += '\techo "" > out\n'
    for mode in itertools.product(*modes):
        mode_str = ", ".join(mode)
        content += f'\techo "Running tests with features: {mode_str}" | tee -a out\n'
        content += f'\tRUST_LOG=info cargo test --features "{mode_str}" -- --test-threads=1 2>&1 | tee -a out\n'

    f = open("Makefile", "r")
    lines = f.readlines()
    f.close()
    in_range = False
    with open("Makefile", "w") as f:
        for line in lines:
            if line.strip() == START_LINE:
                in_range = True
                f.write(START_LINE + "\n")
                f.write(content)
                continue

            if line.strip() == END_LINE:
                in_range = False
                f.write(END_LINE + "\n")
                continue

            if not in_range:
                f.write(line)

    # insert content between start_str and end_str


def update_content(file_path: str, new_content: str):
    f = open(file_path, "r")
    lines = f.readlines()
    f.close()

    in_range = False
    with open(file_path, "w") as f:
        for line in lines:
            if line.strip() == START_LINE:
                in_range = True
                f.write(START_LINE + "\n")
                f.write(new_content + "\n")
                continue

            if line.strip() == END_LINE:
                in_range = False
                f.write(END_LINE + "\n")
                continue

            if not in_range:
                f.write(line)


if __name__ == "__main__":
    options = get_options()

    gen_cargo_features(options)

    # gen_make_test(options)
