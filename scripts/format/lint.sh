#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o nounset

pip install cpplint
pip install "cxc-toolkit>=0.8.2"

PYTHONPATH=. python ./scripts/format/run-cpplint.py
