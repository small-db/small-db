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

    # remove all lines between start_str and end_str
    _ = subprocess.check_output(
        f"sed -i '/{start_str}/,/{end_str}/d' Makefile", shell=True
    )

    # Generate all possible combinations of modes.
    content = "test:\n"
    content += '\techo "" > out\n'
    for mode in itertools.product(*modes):
        mode_str = ", ".join(mode)
        content += f'\t@echo "Running tests with features: {mode_str}" | tee -a out\n'
        content += f'\t@RUST_LOG=info cargo test --features "{mode_str}" -- --test-threads=1 | tee -a out\n'

    # insert content between start_str and end_str
    _ = subprocess.check_output(
        f"sed -i '/{start_str}/a {content}' Makefile", shell=True
    )


if __name__ == "__main__":
    gen_make_test()
