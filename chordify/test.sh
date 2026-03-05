#!/bin/bash

cd target/debug

# Start the first node (bootstrap)
# ./chordify 127.0.0.1:8000 -k 3 -t 1

# sleep 5

# { sleep 5
# echo "insert song1 sd" 
# sleep 2
# echo "query song1"
# } | ./chordify 127.0.0.1:8001 127.0.0.1:8000

gnome-terminal -- bash -c './chordify 127.0.0.1:8000 -k 3 -t 1; exec bash'
#gnome-terminal -- bash -c '{ sleep 6; ../../readfile.sh insert_00_part.txt; sleep 1; echo "query Satisfaction"; } | ./chordify 127.0.0.1:8001 127.0.0.1:8000; exec bash'
gnome-terminal -- bash -c ' time (
    sleep 6
    ../../readfile.sh ../../insert_00_part.txt
    depart
) | ./chordify 127.0.0.1:8001 127.0.0.1:8000; exec bash'