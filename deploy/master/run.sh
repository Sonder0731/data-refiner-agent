#!/usr/bin/env bash
set -Eeuo pipefail

children=()

shutdown() {
  trap - EXIT INT TERM
  if ((${#children[@]})); then
    kill "${children[@]}" 2>/dev/null || true
    wait "${children[@]}" 2>/dev/null || true
  fi
}
trap shutdown EXIT INT TERM

port_open() {
  timeout 2 bash -c "</dev/tcp/$1/$2" 2>/dev/null
}

wait_for_port() {
  local host=$1 port=$2 name=$3
  for _ in $(seq 1 180); do
    if port_open "$host" "$port"; then
      return
    fi
    sleep 2
  done
  echo "Timed out waiting for $name at $host:$port" >&2
  exit 1
}

/usr/local/sbin/hadoop-run.sh &
hadoop_pid=$!
children+=("$hadoop_pid")

wait_for_port master 9870 "HDFS NameNode"
wait_for_port master 8088 "YARN ResourceManager"
wait_for_port master 9083 "Hive Metastore"
wait_for_port master 10000 "HiveServer2"
wait_for_port master 8080 "Spark master"

/opt/data-refiner-api/.venv/bin/uvicorn \
  data_refiner_api.main:app --host 0.0.0.0 --port 7778 &
api_pid=$!
children+=("$api_pid")
wait_for_port 127.0.0.1 7778 "Data Refiner API"

while kill -0 "$hadoop_pid" "$api_pid" 2>/dev/null; do
  for port in 9870 8088 9083 10000 8080; do
    if ! port_open master "$port"; then
      echo "Required master service on port $port stopped responding" >&2
      exit 1
    fi
  done
  if ! port_open 127.0.0.1 7778; then
    echo "Data Refiner API on port 7778 stopped responding" >&2
    exit 1
  fi
  sleep 10
done

echo "A required master process exited" >&2
exit 1
