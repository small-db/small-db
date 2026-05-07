# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "cxc-toolkit>=1.1.2",
# ]
# ///
import argparse
import os
import re
import subprocess
import sys

from scripts import format

# Lines emitted by clang-tidy's frontend that count parsed warnings across the
# whole TU, including filtered third-party headers. Misleading -- suppress.
_NOISE_LINE_RE = re.compile(rb"^\d+ warnings generated\.\s*$")

RUN_CLANG_TIDY_BIN = "run-clang-tidy-18"
DEFAULT_BUILD_DIR = "build/debug"
SOURCE_EXTENSIONS = "cc,cpp,cxx,c++,C"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dir",
        help="directory to run clang-tidy on (default: current directory)",
        default=".",
    )
    parser.add_argument(
        "--extensions",
        help=(
            "comma separated list of source-file extensions "
            "(default: {}; headers are linted via HeaderFilterRegex in .clang-tidy)"
        ).format(SOURCE_EXTENSIONS),
        default=SOURCE_EXTENSIONS,
    )
    parser.add_argument(
        "--build-dir",
        help="directory containing compile_commands.json (default: {})".format(
            DEFAULT_BUILD_DIR
        ),
        default=DEFAULT_BUILD_DIR,
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=os.cpu_count() or 1,
        help="parallel jobs (default: nproc)",
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="apply auto-fixable diagnostics in place; warnings stay non-fatal in this mode",
    )

    args = parser.parse_args()

    db_path = os.path.join(args.build_dir, "compile_commands.json")
    if not os.path.exists(db_path):
        sys.exit(
            f"compile_commands.json not found at {db_path}; "
            "run ./scripts/setup/build.sh first"
        )

    excludes = format.excludes_from_file(format.DEFAULT_CLANG_FORMAT_IGNORE)

    files = format.list_files(
        files=[args.dir],
        exclude=excludes,
        extensions=args.extensions.split(","),
    )

    if not files:
        return

    # Scope header diagnostics to our source tree only. Project-rooted absolute
    # path keeps generated headers under build/ and third-party under cmake/
    # out of the report.
    project_root = os.path.abspath(args.dir)
    header_filter = f"^{re.escape(project_root)}/(src|test)/.*\\.(h|hpp)$"

    argv = [
        RUN_CLANG_TIDY_BIN,
        "-p", args.build_dir,
        "-j", str(args.jobs),
        "-quiet",
        f"-header-filter={header_filter}",
    ]
    if args.fix:
        argv += ["-fix", "-format"]
    else:
        argv += ["-warnings-as-errors=*"]
    argv += [re.escape(os.path.abspath(f)) for f in files]

    proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    assert proc.stdout is not None
    for line in proc.stdout:
        if _NOISE_LINE_RE.match(line):
            continue
        sys.stdout.buffer.write(line)
        sys.stdout.buffer.flush()
    proc.wait()
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()
