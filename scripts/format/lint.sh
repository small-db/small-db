#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o nounset

PYTHONPATH=. uv run ./scripts/format/run-cpplint.py
