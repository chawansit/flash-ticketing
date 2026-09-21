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
    [ "$#" -eq 10 ]
    run_id=$2; manifest=$3; rate=$4; seconds=$5; workers=$6; seat_offset=$7
    max_attempts=$8; retry_base_delay_ms=$9; late_delivery_window_ms=${10}
    run="/root/unattended-$run_id"
    public="$run/public"
    private="$run/private"
    mkdir -p "$public/load" "$private"
    chmod 700 "$private"
    cp "$manifest" "$private/manifest.json"
    chmod 600 "$private/manifest.json"
    "$python_bin" scripts/warm_capacity_manifest.py --manifest "$private/manifest.json" --output "$public/warmup.json"
    "$python_bin" scripts/parallel_cloud_load.py --manifest "$private/manifest.json" --output "$public/load" --rate "$rate" --seconds "$seconds" --workers "$workers" --seat-offset "$seat_offset" --transport-diagnostics --keepalive-expiry 5 --start-delay 15 \
      --max-attempts "$max_attempts" --retry-base-delay-ms "$retry_base_delay_ms" \
      --late-delivery-window-ms "$late_delivery_window_ms" > "$public/coordinator.log" 2>&1
    ;;
  start)
    [ "$#" -eq 10 ]
    run_id=$2; manifest=$3; rate=$4; seconds=$5; workers=$6; seat_offset=$7
    max_attempts=$8; retry_base_delay_ms=$9; late_delivery_window_ms=${10}
    run="/root/unattended-$run_id"
    public="$run/public"
    private="$run/private"
    mkdir -p "$public" "$private"
    chmod 700 "$private"
    [ -f "$manifest" ]
    [ ! -e "$public/job-status.json" ]
    [ ! -e "$private/job.pid" ]
    limit=$((seconds + 180))
    nohup setsid sh -c '
      helper=$1; status=$2; limit=$3; shift 3
      timeout -k 10 "$limit" sh "$helper" run "$@"
      code=$?
      printf "{\"state\":\"finished\",\"exit_code\":%s}\n" "$code" > "$status.tmp"
      mv "$status.tmp" "$status"
      exit "$code"
    ' sh "$repo/scripts/huawei_capacity_generator.sh" "$public/job-status.json" "$limit" \
      "$run_id" "$manifest" "$rate" "$seconds" "$workers" "$seat_offset" \
      "$max_attempts" "$retry_base_delay_ms" "$late_delivery_window_ms" \
      </dev/null > "$public/job-control.log" 2>&1 &
    printf '%s\n' "$!" > "$private/job.pid"
    printf '{"state":"started"}\n'
    ;;
  status)
    [ "$#" -eq 2 ]
    run="/root/unattended-$2"
    status="$run/public/job-status.json"
    pid_file="$run/private/job.pid"
    if [ -f "$status" ]; then
      cat "$status"
    elif [ -f "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
      printf '{"state":"running"}\n'
    else
      printf '{"state":"missing"}\n'
      exit 1
    fi
    ;;
  stop)
    [ "$#" -eq 2 ]
    run="/root/unattended-$2"
    status="$run/public/job-status.json"
    pid_file="$run/private/job.pid"
    if [ ! -f "$status" ]; then
      [ -f "$pid_file" ]
      pid=$(cat "$pid_file")
      case "$pid" in ''|*[!0-9]*) exit 1 ;; esac
      if kill -0 "$pid" 2>/dev/null; then
        pgid=$(sed 's/^.*) //' "/proc/$pid/stat" | awk '{print $3}')
        [ "$pgid" = "$pid" ]
        /bin/kill -TERM -- "-$pgid" 2>/dev/null || true
        attempt=0
        while kill -0 "$pid" 2>/dev/null && [ "$attempt" -lt 20 ]; do
          attempt=$((attempt + 1))
          sleep 0.2
        done
        /bin/kill -KILL -- "-$pgid" 2>/dev/null || true
      fi
      [ -f "$status" ] || printf '{"state":"stopped","exit_code":-1}\n' > "$status"
    fi
    ;;
  cleanup)
    [ "$#" -eq 2 ]
    run="/root/unattended-$2"
    if [ -f "$run/private/job.pid" ] && [ ! -f "$run/public/job-status.json" ] \
      && kill -0 "$(cat "$run/private/job.pid")" 2>/dev/null; then
      echo "Cannot clean up a running generator job" >&2
      exit 1
    fi
    rm -rf "$run/private"
    rm -f "/root/unattended-$2-upload.json"
    ;;
  *)
    echo "usage: $0 {run|start|status|stop|cleanup} ..." >&2
    exit 2
    ;;
esac
