#!/bin/bash

input_file="$1"

while read -r x; do
    y=$(tr -dc 'a-zA-Z0-9' </dev/urandom | head -c 8)
    echo "insert $x $y"
done < "$input_file"