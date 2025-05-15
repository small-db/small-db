#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

source ~/code/python_env_xiaochen/bin/activate

# pip install matplotlib
python ./scripts/benchmark/benchmark.py
python ./scripts/benchmark/draw.py