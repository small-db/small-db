#!/usr/bin/env bash

set -o errexit

uv run ./scripts/format/run-clang-format.py
uv run ./scripts/format/run-cpplint.py
uv run ./scripts/format/run-clang-tidy.py --fix
