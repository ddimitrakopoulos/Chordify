#!/bin/bash

cd target/debug

gnome-terminal -- bash -c './chordify 127.0.0.1:8000 -k 3 -t 1; exec bash'

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../readfile.sh ../../insert_00_part.txt
    echo "depart"
) | ./chordify 127.0.0.1:8001 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../readfile.sh ../../insert_00_part.txt
    echo "depart"
) | ./chordify 127.0.0.1:8002 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../readfile.sh ../../insert_00_part.txt
    echo "depart"
) | ./chordify 127.0.0.1:8003 127.0.0.1:8000; exec bash' &





