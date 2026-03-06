#!/bin/bash

cd ../target/debug

k="$1"
t="$2"

#gnome-terminal -- bash -c 'echo "k=$1 t=$2"; ./chordify 127.0.0.1:8000 -k $1 -t $2; exec bash' _ "$k" "$t"

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile2.sh ../../data/query/query_00.txt
    echo "depart"
) | ./chordify 127.0.0.1:8001 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile2.sh ../../data/query/query_01.txt
    echo "depart"
) | ./chordify 127.0.0.1:8002 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile2.sh ../../data/query/query_02.txt
    echo "depart"
) | ./chordify 127.0.0.1:8003 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile2.sh ../../data/query/query_03.txt
    echo "depart"
) | ./chordify 127.0.0.1:8004 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile2.sh ../../data/query/query_04.txt
    echo "depart"
) | ./chordify 127.0.0.1:8005 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile2.sh ../../data/query/query_05.txt
    echo "depart"
) | ./chordify 127.0.0.1:8006 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile2.sh ../../data/query/query_06.txt
    echo "depart"
) | ./chordify 127.0.0.1:8007 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile2.sh ../../data/query/query_07.txt
    echo "depart"
) | ./chordify 127.0.0.1:8008 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile2.sh ../../data/query/query_08.txt
    echo "depart"
) | ./chordify 127.0.0.1:8009 127.0.0.1:8000; exec bash' &

gnome-terminal -- bash -c ' time (
    sleep 6
    ../../experiments/readfile2.sh ../../data/query/query_09.txt
    echo "depart"
) | ./chordify 127.0.0.1:8010 127.0.0.1:8000; exec bash' &




