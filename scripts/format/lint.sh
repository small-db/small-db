#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o nounset

# install uv
# curl -LsSf https://astral.sh/uv/install.sh | sh
# uv tool install cpplint
pip install cpplint
pip install "cxc-toolkit>=0.8.2"

# run cpplint
# export PYTHONPATH=.
python ./scripts/format/run-cpplint.py