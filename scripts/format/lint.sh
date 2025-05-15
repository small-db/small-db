#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o nounset

# install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
uv tool install cpplint

# run cpplint
export PYTHONPATH=.
uv run ./scripts/format/run-cpplint.py