#!/usr/bin/env bash

# disable xtrace to make the output shorter
# set -o xtrace
set -o errexit
set -o nounset
set -o pipefail

source ~/code/python_env_xiaochen/bin/activate

# pip install matplotlib
python ./scripts/benchmark/benchmark.py
python ./scripts/benchmark/draw.py