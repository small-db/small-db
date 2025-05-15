#!/usr/bin/env bash

set -o errexit

source ~/code/python_env_xiaochen/bin/activate

python3 -m scripts.format.run-clang-format
python3 -m scripts.format.run-cpplint
