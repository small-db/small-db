#!/usr/bin/env bash
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "cpplint>=2.0.2",
#     "cxc-toolkit>=1.1.2",
# ]
# ///

set -o xtrace
set -o errexit
set -o nounset

pip install cpplint
pip install "cxc-toolkit>=0.8.2"

PYTHONPATH=. python ./scripts/format/run-cpplint.py
