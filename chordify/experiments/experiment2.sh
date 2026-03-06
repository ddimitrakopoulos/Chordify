#!/bin/bash

cd ../target/debug

k="$1"
t="$2"

gnome-terminal -- bash -c 'echo "k=$1 t=$2"; ./chordify 127.0.0.1:4000 -k $1 -t $2; exec bash' _ "$k" "$t"

sleep 2

gnome-terminal -- bash -c './query_throughput  \
  --addr 127.0.0.1:5000 \
  --bootstrap 127.0.0.1:4000 \
  --insert-file ../../data/insert/insert_00_part.txt\
  --query-file ../../data/queries/query_00.txt; exec bash' &

gnome-terminal -- bash -c './query_throughput  \
  --addr 127.0.0.1:5001 \
  --bootstrap 127.0.0.1:4000 \
  --insert-file ../../data/insert/insert_01_part.txt\
  --query-file ../../data/queries/query_01.txt; exec bash' &

gnome-terminal -- bash -c './query_throughput  \
  --addr 127.0.0.1:5002 \
  --bootstrap 127.0.0.1:4000 \
  --insert-file ../../data/insert/insert_02_part.txt\
  --query-file ../../data/queries/query_02.txt; exec bash' &

gnome-terminal -- bash -c './query_throughput  \
  --addr 127.0.0.1:5003 \
  --bootstrap 127.0.0.1:4000 \
  --insert-file ../../data/insert/insert_03_part.txt\
  --query-file ../../data/queries/query_03.txt; exec bash' &

gnome-terminal -- bash -c './query_throughput  \
  --addr 127.0.0.1:5004 \
  --bootstrap 127.0.0.1:4000 \
  --insert-file ../../data/insert/insert_04_part.txt\
  --query-file ../../data/queries/query_04.txt; exec bash' &

gnome-terminal -- bash -c './query_throughput  \
  --addr 127.0.0.1:5005 \
  --bootstrap 127.0.0.1:4000 \
  --insert-file ../../data/insert/insert_05_part.txt\
  --query-file ../../data/queries/query_05.txt; exec bash' &

gnome-terminal -- bash -c './query_throughput  \
  --addr 127.0.0.1:5006 \
  --bootstrap 127.0.0.1:4000 \
  --insert-file ../../data/insert/insert_06_part.txt\
  --query-file ../../data/queries/query_06.txt; exec bash' &

gnome-terminal -- bash -c './query_throughput  \
  --addr 127.0.0.1:5007 \
  --bootstrap 127.0.0.1:4000 \
  --insert-file ../../data/insert/insert_07_part.txt\
  --query-file ../../data/queries/query_07.txt; exec bash' &

gnome-terminal -- bash -c './query_throughput  \
  --addr 127.0.0.1:5008 \
  --bootstrap 127.0.0.1:4000 \
  --insert-file ../../data/insert/insert_08_part.txt\
  --query-file ../../data/queries/query_08.txt; exec bash' &

gnome-terminal -- bash -c './query_throughput  \
  --addr 127.0.0.1:5009 \
  --bootstrap 127.0.0.1:4000 \
  --insert-file ../../data/insert/insert_09_part.txt\
  --query-file ../../data/queries/query_09.txt; exec bash' &