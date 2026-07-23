#!/usr/bin/env bash
# Runs embed -> build -> stability for the generalised dictionary, then idles.
set -u
cd "$(dirname "$0")/.."
LOG=pipeline_output/generalised_build_log.txt
{
  echo "=== embed started $(date -u) ==="
  python3 pipeline/build_dictionaries.py embed 2>&1
  echo "=== embed done $(date -u) ==="
  echo "=== build started $(date -u) ==="
  python3 pipeline/build_dictionaries.py build 2>&1
  echo "=== build done $(date -u) ==="
  echo "=== stability started $(date -u) ==="
  python3 pipeline/build_dictionaries.py stability 2>&1
  echo "=== hygiene pass started $(date -u) ==="
  python3 pipeline/clean_dictionary.py 2>&1
  echo "=== ALL STAGES DONE $(date -u) ==="
} | tee "$LOG"
sleep infinity
