# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "cxc-toolkit",
# ]
# ///
import argparse
import sys

import cxc_toolkit


from scripts import format


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dir",
        help="directory to run clang-format on (default: current directory)",
        default=".",
    )
    parser.add_argument(
        "--extensions",
        help="comma separated list of file extensions (default: {})".format(
            format.DEFAULT_EXTENSIONS
        ),
        default=format.DEFAULT_EXTENSIONS,
    )

    args = parser.parse_args()

    excludes = format.excludes_from_file(format.DEFAULT_CLANG_FORMAT_IGNORE)

    files = format.list_files(
        files=[args.dir],
        exclude=excludes,
        extensions=args.extensions.split(","),
    )

    for file in files:
        cxc_toolkit.exec.run_command(f"cpplint {file}", raise_on_failure=True)


if __name__ == "__main__":
    main()
