#!/usr/bin/env bash
set -Eeuo pipefail

BOOTSTRAP_DIR=/opt/data-refiner-bootstrap

ready=false
for _ in $(seq 1 180); do
  if hdfs dfsadmin -safemode get 2>/dev/null | grep -q 'Safe mode is OFF' \
    && hdfs dfsadmin -report 2>/dev/null | grep -Eq 'Live datanodes \([2-9][0-9]*\)'; then
    ready=true
    break
  fi
  sleep 2
done

if [[ "$ready" != true ]]; then
  echo "HDFS did not leave safe mode with two live DataNodes" >&2
  exit 1
fi

spark-sql --master 'local[1]' -e \
  "CREATE DATABASE IF NOT EXISTS default LOCATION 'hdfs:///user/hive/warehouse'"
spark-sql --master 'local[1]' -e \
  "CREATE DATABASE IF NOT EXISTS temp LOCATION 'hdfs:///user/hive/warehouse/temp.db'"

hdfs dfs -mkdir -p \
  /data /checkpoints /jars /python_scripts \
  /user/hive/warehouse /user/hive/warehouse/temp.db
hdfs dfs -chmod 755 /data /checkpoints /jars /python_scripts /user/hive/warehouse
hdfs dfs -chmod 775 /user/hive/warehouse/temp.db
hdfs dfs -chown -R jupyter:supergroup \
  /data /checkpoints /jars /python_scripts /user/hive/warehouse

hdfs dfs -put -f "$BOOTSTRAP_DIR/run_cluster_args.py" \
  /python_scripts/run_cluster_args.py
hdfs dfs -put -f \
  "$SPARK_HOME/jars/graphframes-0.8.4-spark3.5-s_2.12.jar" \
  /jars/graphframes-0.8.4-spark3.5-s_2.12.jar
hdfs dfs -chmod 644 \
  /python_scripts/run_cluster_args.py \
  /jars/graphframes-0.8.4-spark3.5-s_2.12.jar

verify_hdfs_file() {
  local target=$1 expected_size=$2 expected_sha=$3
  local actual_size actual_sha marker marker_value modification_time
  actual_size=$(hdfs dfs -stat '%b' "$target")
  [[ "$actual_size" == "$expected_size" ]] || return 1
  modification_time=$(hdfs dfs -stat '%Y' "$target")
  marker="/.data-refiner-bootstrap${target}.verified"
  marker_value="$expected_sha $expected_size $modification_time"
  if hdfs dfs -test -e "$marker" \
    && [[ $(hdfs dfs -cat "$marker") == "$marker_value" ]]; then
    return
  fi

  actual_sha=$(hdfs dfs -cat "$target" | sha256sum | cut -d' ' -f1)
  [[ "$actual_sha" == "$expected_sha" ]] || return 1
  hdfs dfs -mkdir -p "${marker%/*}"
  printf '%s' "$marker_value" | hdfs dfs -put -f - "$marker"
}

while IFS=$'\t' read -r url target expected_size expected_sha; do
  target_dir=${target%/*}
  hdfs dfs -mkdir -p "$target_dir"

  if hdfs dfs -test -e "$target"; then
    if ! verify_hdfs_file "$target" "$expected_size" "$expected_sha"; then
      echo "Existing HDFS file does not match manifest: $target" >&2
      exit 1
    fi
    hdfs dfs -setrep -w 2 "$target"
    hdfs dfs -chmod 644 "$target"
    hdfs dfs -chown jupyter:supergroup "$target"
    echo "Verified $target"
    continue
  fi

  local_file=$(mktemp)
  trap 'rm -f "$local_file"' EXIT
  curl --fail --location --retry 5 --retry-all-errors \
    --output "$local_file" "$url"
  [[ $(stat -c '%s' "$local_file") == "$expected_size" ]] \
    || { echo "Downloaded size mismatch for $target" >&2; exit 1; }
  echo "$expected_sha  $local_file" | sha256sum --check --status \
    || { echo "Downloaded SHA-256 mismatch for $target" >&2; exit 1; }

  temporary_target="${target}.upload.$$"
  hdfs dfs -put "$local_file" "$temporary_target"
  hdfs dfs -mv "$temporary_target" "$target"
  rm -f "$local_file"
  trap - EXIT

  hdfs dfs -setrep -w 2 "$target"
  hdfs dfs -chmod 644 "$target"
  hdfs dfs -chown jupyter:supergroup "$target"
  verify_hdfs_file "$target" "$expected_size" "$expected_sha"
  echo "Installed $target"
done < "$BOOTSTRAP_DIR/datasets.tsv"

echo "Data Refiner bootstrap completed"
