#!/usr/bin/env bash

set -o xtrace
set -o errexit
set -o nounset
set -o pipefail

cc -c ./dirty/fake_clock_gettime.c -fPIE -O2 -o ./dirty/fake_clock_gettime.o
# cc -c ./dirty/fake_clock_gettime.c -fPIE -O2 -g -o ./dirty/fake_clock_gettime.o

mkdir -p build
go build -o build/main ./main.go

# get pid of the process "./target/debug/clock"
PID=$(pgrep clock)
sudo ./build/main --pid $PID