#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o nounset

# install uv
# curl -LsSf https://astral.sh/uv/install.sh | sh
# uv tool install cpplint
pip install cpplint
pip install cxc-toolkit

# run cpplint
export PYTHONPATH=.
uv run ./scripts/format/run-cpplint.py