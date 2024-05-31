import itertools
import subprocess


def gen_make_test():
    modes = [
        ["tree_latch", "page_latch"],
        ["aries_steal", "aries_no_steal"],
        ["aries_force", "aries_no_force"],
    ]

    # remove old content
    start_str = "# [MAKE TEST START]"
    end_str = "# [MAKE TEST END]"

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
            if line.strip() == start_str:
                in_range = True
                f.write(start_str + "\n")
                f.write(content)
                continue

            if line.strip() == end_str:
                in_range = False
                f.write(end_str + "\n")
                continue

            if not in_range:
                f.write(line)

    # insert content between start_str and end_str


if __name__ == "__main__":
    gen_make_test()
