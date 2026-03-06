#!/bin/bash

cd ../target/debug

k="$1"
t="$2"

gnome-terminal -- bash -c 'echo "k=$1 t=$2"; ./chordify 127.0.0.1:8000 -k $1 -t $2; exec bash' _ "$k" "$t"

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile.sh ../../experiments/insert/insert_00_part.txt
    echo "depart"
) | ./chordify 127.0.0.1:8001 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile.sh ../../experiments/insert/insert_01_part.txt
    echo "depart"
) | ./chordify 127.0.0.1:8002 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile.sh ../../experiments/insert/insert_02_part.txt
    echo "depart"
) | ./chordify 127.0.0.1:8003 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile.sh ../../experiments/insert/insert_03_part.txt
    echo "depart"
) | ./chordify 127.0.0.1:8004 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile.sh ../../experiments/insert/insert_04_part.txt
    echo "depart"
) | ./chordify 127.0.0.1:8005 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile.sh ../../experiments/insert/insert_05_part.txt
    echo "depart"
) | ./chordify 127.0.0.1:8006 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile.sh ../../experiments/insert/insert_06_part.txt
    echo "depart"
) | ./chordify 127.0.0.1:8007 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile.sh ../../experiments/insert/insert_07_part.txt
    echo "depart"
) | ./chordify 127.0.0.1:8008 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile.sh ../../experiments/insert/insert_08_part.txt
    echo "depart"
) | ./chordify 127.0.0.1:8009 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile.sh ../../experiments/insert/insert_09_part.txt
    echo "depart"
) | ./chordify 127.0.0.1:8010 127.0.0.1:8000; exec bash' &




