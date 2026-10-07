#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

if [[ ! -f .env ]]; then
  cp .env.example .env
fi

if [[ ! -f .runtime.env ]]; then
  umask 077
  database_password=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
  container_api_key=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')
  {
    echo "DATA_REFINER_DB_PASSWORD=$database_password"
    echo "DOCKER_CONTAINER_API_KEY=$container_api_key"
  } > .runtime.env
fi

compose=(docker compose --env-file .env --env-file .runtime.env)
hadoop_ref=${HADOOP_STACK_REF:-$(sed -n 's/^HADOOP_STACK_REF=//p' .env | tail -n 1)}
hadoop_ref=${hadoop_ref:-de5634fc70d636c154c30dfd996402c38c1ef155}
hadoop_repo=https://github.com/Sonder0731/hadoop-hive-spark-docker.git

rebuild_hadoop=false
for image in \
  hadoop-hive-spark-base \
  hadoop-hive-spark-master \
  hadoop-hive-spark-worker \
  hadoop-hive-spark-history; do
  installed_ref=$(docker image inspect "$image" \
    --format '{{ index .Config.Labels "data-refiner.hadoop-ref" }}' \
    2>/dev/null || true)
  if [[ "$installed_ref" != "$hadoop_ref" ]]; then
    rebuild_hadoop=true
    break
  fi
done

if [[ "$rebuild_hadoop" == true ]]; then
  docker build --label "data-refiner.hadoop-ref=$hadoop_ref" \
    -t hadoop-hive-spark-base "$hadoop_repo#$hadoop_ref:base"
  docker build --label "data-refiner.hadoop-ref=$hadoop_ref" \
    -t hadoop-hive-spark-master "$hadoop_repo#$hadoop_ref:master"
  docker build --label "data-refiner.hadoop-ref=$hadoop_ref" \
    -t hadoop-hive-spark-worker "$hadoop_repo#$hadoop_ref:worker"
  docker build --label "data-refiner.hadoop-ref=$hadoop_ref" \
    -t hadoop-hive-spark-history "$hadoop_repo#$hadoop_ref:history"
fi

"${compose[@]}" --profile images build
"${compose[@]}" up -d --wait --wait-timeout 900 \
  metastore data-refiner-postgres embedding-api master worker1 worker2 history \
  container-api spark-monitor
"${compose[@]}" --profile bootstrap run --rm bootstrap

echo "Data Refiner deployment completed."
