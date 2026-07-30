#!/usr/bin/env sh
set -eu

i=1

while [ "$i" -le 10 ]; do
    echo "Hello, world!"
    sleep 2
    i=$((i + 1))
done