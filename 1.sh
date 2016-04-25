#/usr/bin/env bash
set -v

time ./src/tarantool test-1.lua
time ./src/tarantool test-2.lua
