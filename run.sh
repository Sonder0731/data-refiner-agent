#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

if (($# == 0)); then
  echo 'Usage: ./run.sh "<request>"' >&2
  echo '       ./run.sh --thread-id <id> --resume "<answer>"' >&2
  exit 2
fi

if [[ ! -f .env || ! -f .runtime.env ]]; then
  echo "Deployment configuration is missing. Run ./deploy.sh first." >&2
  exit 2
fi

configured_openai_key=$(sed -n 's/^OPENAI_API_KEY=//p' .env | tail -n 1)
if [[ -z "${OPENAI_API_KEY:-$configured_openai_key}" ]]; then
  echo "Set OPENAI_API_KEY in .env or export it before running." >&2
  exit 2
fi

compose=(docker compose --env-file .env --env-file .runtime.env)
if [[ $("${compose[@]}" ps --status running --services master) != master ]]; then
  echo "Data Refiner is not running. Run ./deploy.sh first." >&2
  exit 1
fi

"${compose[@]}" run --rm --no-deps agent "$@"
