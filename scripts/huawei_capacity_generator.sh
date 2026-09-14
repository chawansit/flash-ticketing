#!/bin/sh
set -eu

repo=${FLASH_TICKETING_GENERATOR_DIR:-/root/flash-generator}
python_bin=${FLASH_TICKETING_LOAD_PYTHON:-}
if [ -z "$python_bin" ]; then
  if [ -x /root/http-load-venv/bin/python ]; then
    python_bin=/root/http-load-venv/bin/python
  else
    python_bin=python3
  fi
fi
cd "$repo"

case "${1:-}" in
  run)
    [ "$#" -eq 7 ]
    run_id=$2; manifest=$3; rate=$4; seconds=$5; workers=$6; seat_offset=$7
    run="/root/unattended-$run_id"
    public="$run/public"
    private="$run/private"
    mkdir -p "$public/load" "$private"
    chmod 700 "$private"
    cp "$manifest" "$private/manifest.json"
    chmod 600 "$private/manifest.json"
    "$python_bin" scripts/warm_capacity_manifest.py --manifest "$private/manifest.json" --output "$public/warmup.json"
    "$python_bin" scripts/parallel_cloud_load.py --manifest "$private/manifest.json" --output "$public/load" --rate "$rate" --seconds "$seconds" --workers "$workers" --seat-offset "$seat_offset" --transport-diagnostics --keepalive-expiry 5 --start-delay 15 > "$public/coordinator.log" 2>&1
    ;;
  cleanup)
    [ "$#" -eq 2 ]
    rm -rf "/root/unattended-$2/private"
    rm -f "/root/unattended-$2-upload.json"
    ;;
  *)
    echo "usage: $0 {run|cleanup} ..." >&2
    exit 2
    ;;
esac
