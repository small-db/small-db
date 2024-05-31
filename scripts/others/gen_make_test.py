import itertools


def gen_make_test():
    modes = [
        ["tree_latch", "page_latch"],
        ["aries_steal", "aries_no_steal"],
        ["aries_force", "aries_no_force"],
    ]

    # Generate all possible combinations of modes.
    print("test:")
    print("touch out")
    for mode in itertools.product(*modes):
        mode_str = ", ".join(mode)
        print(f'echo "Running tests with features: {mode_str}" | tee -a out')
        print(
            f'RUST_LOG=info cargo test --features "{mode_str}" -- --test-threads=1 | tee -a out'
        )


if __name__ == "__main__":
    gen_make_test()
