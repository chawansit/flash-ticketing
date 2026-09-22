#!/bin/sh
set -eu

repo=${FLASH_TICKETING_BACKEND_DIR:-/root/flash-ticketing-rds}
env_file=${FLASH_TICKETING_RDS_ENV:-.env.rds}
cd "$repo"
compose="docker compose --env-file $env_file -f compose.yaml -f compose.rds.yaml -f compose.horizontal.yaml -f compose.keepalive10.yaml"

run_paths() {
  run_id=$1
  run="$repo/tmp/unattended-$run_id"
  private="$run/private"
  public="$run/public"
  raw="$run/raw"
  mkdir -p "$private" "$public" "$raw"
  chmod 700 "$private"
}

api_id() {
  $compose ps -q api | head -n 1
}

wait_apis() {
  attempt=0
  while [ "$attempt" -lt 90 ]; do
    total=0
    healthy=0
    for id in $($compose ps -q api); do
      total=$((total + 1))
      state=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$id")
      [ "$state" = healthy ] && healthy=$((healthy + 1))
    done
    [ "$total" -eq 4 ] && [ "$healthy" -eq 4 ] && return 0
    attempt=$((attempt + 1))
    sleep 2
  done
  return 1
}

wait_maintenance() {
  expected=$1
  attempt=0
  while [ "$attempt" -lt 90 ]; do
    total=0
    running=0
    for id in $($compose ps -q maintenance); do
      total=$((total + 1))
      state=$(docker inspect -f '{{.State.Status}}' "$id")
      [ "$state" = running ] && running=$((running + 1))
    done
    [ "$total" -eq "$expected" ] && [ "$running" -eq "$expected" ] && return 0
    attempt=$((attempt + 1))
    sleep 2
  done
  return 1
}

set_admission() {
  value=$1
  if grep -q '^API_ADMISSION_PER_INSTANCE=' "$env_file"; then
    sed -i "s/^API_ADMISSION_PER_INSTANCE=.*/API_ADMISSION_PER_INSTANCE=$value/" "$env_file"
  else
    printf 'API_ADMISSION_PER_INSTANCE=%s\n' "$value" >> "$env_file"
  fi
}

case "${1:-}" in
  prepare)
    [ "$#" -eq 7 ]
    run_paths "$2"
    shows=$3; seats=$4; sale_hours=$5; origin=$6; viewers=$7
    api=$(api_id)
    docker exec -u 0 "$api" sh -lc 'mkdir -p /app/tmp && chown 10001:10001 /app/tmp && chmod 700 /app/tmp && rm -f /tmp/capacity-fixture.json /app/tmp/private-load-manifest.json'
    docker exec "$api" sh -lc 'cd /app && TEST_DATABASE_URL="$DATABASE_URL" TEST_REDIS_URL="$REDIS_URL" python scripts/prepare_capacity_fixture.py --output /tmp/capacity-fixture.json --shows "$1" --seats "$2" --sale-hours "$3"' sh "$shows" "$seats" "$sale_hours"
    docker exec "$api" sh -lc 'cd /app && python scripts/export_load_manifest.py --results /tmp/capacity-fixture.json --origin "$1" --output /app/tmp/private-load-manifest.json --viewers "$2" --seat-offset 0' sh "$origin" "$viewers"
    docker cp "$api":/app/tmp/private-load-manifest.json "$private/manifest.json"
    docker cp "$api":/tmp/capacity-fixture.json "$public/fixture.json"
    chmod 600 "$private/manifest.json"
    ;;
  deploy)
    [ "$#" -eq 6 ]
    run_paths "$2"
    candidate=$3; fallback=$4; maintenance_candidate=$5; maintenance_fallback=$6
    original_maintenance=$($compose ps -q maintenance | wc -l | tr -d ' ')
    [ "$original_maintenance" -eq "$maintenance_fallback" ]
    printf '%s\n' "$original_maintenance" > "$private/original-maintenance"
    chmod 600 "$private/original-maintenance"
    printf '%s\n' "$fallback" > "$private/original-admission"
    chmod 600 "$private/original-admission"
    set_admission "$candidate"
    $compose build api migrate
    $compose up -d --no-deps --force-recreate --scale api=4 api
    wait_apis
    $compose up -d --no-deps --scale "maintenance=$maintenance_candidate" maintenance
    wait_maintenance "$maintenance_candidate"
    source_hash=$(sha256sum src/ticketing/infrastructure/postgres.py | cut -d " " -f 1)
    for id in $($compose ps -q api); do
      image_hash=$(docker exec "$id" sha256sum /app/src/ticketing/infrastructure/postgres.py | cut -d " " -f 1)
      [ "$image_hash" = "$source_hash" ]
      docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$id" |
        grep -qx "RESERVE_CONCURRENCY=$candidate"
    done
    printf '{"candidate_admission":%s,"api_replicas":4,"db_pool_per_api":3,"maintenance_replicas":%s,"pass":true}\n' "$candidate" "$maintenance_candidate" > "$public/deployment.json"
    ;;
  preflight)
    [ "$#" -eq 3 ]
    run_paths "$2"
    seconds=$3
    api=$(api_id)
    docker cp "$private/manifest.json" "$api":/tmp/private-load-manifest.json
    docker exec -u 0 "$api" chown 10001:10001 /tmp/private-load-manifest.json
    docker exec -u 0 "$api" rm -f /tmp/capacity-preflight.json /tmp/capacity-queue-before.json
    docker exec "$api" sh -lc 'cd /app && STAGE_DATABASE_URL="$DATABASE_URL" python scripts/cloud_load_preflight.py --manifest /tmp/private-load-manifest.json --seconds "$1" --output /tmp/capacity-preflight.json' sh "$seconds"
    docker exec "$api" sh -lc 'cd /app && STAGE_DATABASE_URL="$DATABASE_URL" python scripts/capacity_queue_state.py --manifest /tmp/private-load-manifest.json --output /tmp/capacity-queue-before.json'
    docker cp "$api":/tmp/capacity-preflight.json "$public/preflight.json"
    docker cp "$api":/tmp/capacity-queue-before.json "$public/queue-before.json"
    ;;
  observe)
    [ "$#" -eq 3 ]
    run_paths "$2"
    seconds=$3
    date -u '+%Y-%m-%dT%H:%M:%SZ' > "$private/observe-start"
    urls=""
    for id in $($compose ps -q api); do
      ip=$(docker inspect -f '{{(index .NetworkSettings.Networks "flash-ticketing_default").IPAddress}}' "$id")
      urls="$urls --url http://$ip:8000/metrics"
    done
    nohup python3 scripts/cloud_pressure_observe.py $urls --seconds "$seconds" --interval 0.5 --output "$raw/pressure.ndjson" > "$raw/pressure.log" 2>&1 &
    echo $! > "$private/pressure.pid"
    api=$(api_id)
    nohup docker exec "$api" python /app/scripts/pgbouncer_pressure_observe.py --seconds "$seconds" --interval 0.2 --output /tmp/capacity-pgbouncer.jsonl > "$raw/pgbouncer.log" 2>&1 &
    echo $! > "$private/pgbouncer.pid"
    nohup docker exec "$api" sh -lc 'TEST_DATABASE_URL="$DATABASE_URL" API_METRICS_URL=http://127.0.0.1:8000/metrics python /app/scripts/cloud_benchmark_observe.py --fixtures /tmp/private-load-manifest.json --output /tmp/capacity-backend.json --seconds "$1"' sh "$seconds" > "$raw/backend.log" 2>&1 &
    echo $! > "$private/backend.pid"
    nohup $compose run --rm --no-deps -T --name "ft-rds-wait-$2" --user root \
      -v "$raw:/evidence" migrate sh -lc \
      'unset PGSSLROOTCERT; RDS_DATABASE_URL="$DATABASE_URL" python /app/scripts/rds_wait_observe.py --seconds "$1" --interval 0.1 --output /evidence/rds-waits.jsonl' \
      sh "$seconds" > "$raw/rds-waits.log" 2>&1 &
    echo $! > "$private/rds-waits.pid"
    ;;
  stop-observers)
    [ "$#" -eq 2 ]
    run_paths "$2"
    for file in "$private"/*.pid; do
      [ -f "$file" ] || continue
      pid=$(cat "$file")
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    done
    docker rm -f "ft-rds-wait-$2" >/dev/null 2>&1 || true
    observer_failed=0
    if [ -s "$raw/rds-waits.jsonl" ]; then
      python3 scripts/summarize_rds_waits.py "$raw/rds-waits.jsonl" > \
        "$public/rds-waits-summary.json" || observer_failed=1
    else
      observer_failed=1
    fi
    api=$(api_id)
    docker cp "$api":/tmp/capacity-pgbouncer.jsonl "$raw/pgbouncer.jsonl" 2>/dev/null || true
    if docker cp "$api":/tmp/capacity-backend.json "$raw/backend.json" 2>/dev/null; then
      python3 scripts/summarize_drain_trace.py "$raw/backend.json" > "$public/drain-trace-summary.json" || observer_failed=1
    else
      observer_failed=1
    fi
    since=$(cat "$private/observe-start")
    index=0
    for id in $($compose ps -q api); do
      docker logs --since "$since" "$id" 2>&1 |
        python3 scripts/extract_capacity_api_errors.py --limit 20 > "$public/api-errors-$index.json"
      docker logs --since "$since" "$id" 2>&1 |
        python3 scripts/summarize_slow_db_phases.py --limit 50 > "$public/db-slow-$index.json"
      index=$((index + 1))
    done
    [ "$index" -eq 4 ] && [ "$observer_failed" -eq 0 ]
    ;;
  audit)
    [ "$#" -eq 2 ]
    run_paths "$2"
    api=$(api_id)
    docker exec -u 0 "$api" rm -rf /tmp/capacity-load /tmp/capacity-durability.json
    docker cp "$public/load" "$api":/tmp/capacity-load
    audit_status=0
    docker exec "$api" sh -lc 'cd /app && TEST_DATABASE_URL="$DATABASE_URL" python scripts/verify_cloud_holds.py --results /tmp/capacity-load --output /tmp/capacity-durability.json' || audit_status=$?
    docker cp "$api":/tmp/capacity-durability.json "$public/durability.json"
    exit "$audit_status"
    ;;
  gate)
    [ "$#" -eq 2 ]
    run_paths "$2"
    rejected=0
    for id in $($compose ps -q api); do
      ip=$(docker inspect -f '{{(index .NetworkSettings.Networks "flash-ticketing_default").IPAddress}}' "$id")
      value=$(curl -fsS "http://$ip:8000/metrics" | awk '/ticketing_hold_admission_total\{outcome="rejected"\}/ {print $2}')
      value=${value:-0}
      rejected=$(awk -v total="$rejected" -v item="$value" 'BEGIN {print total + item}')
    done
    pass=false
    [ "$rejected" = 0 ] && pass=true
    printf '{"admission_rejections":%s,"pass":%s}\n' "$rejected" "$pass" > "$public/admission.json"
    [ "$rejected" = 0 ]
    ;;
  rollback)
    [ "$#" -eq 2 ]
    run_paths "$2"
    original=$(cat "$private/original-admission")
    original_maintenance=$(cat "$private/original-maintenance")
    set_admission "$original"
    $compose up -d --no-deps --force-recreate --scale api=4 api
    wait_apis
    $compose up -d --no-deps --scale "maintenance=$original_maintenance" maintenance
    wait_maintenance "$original_maintenance"
    printf '{"restored_admission":%s,"api_replicas":4,"maintenance_replicas":%s,"pass":true}\n' "$original" "$original_maintenance" > "$public/rollback.json"
    ;;
  cleanup)
    [ "$#" -eq 2 ]
    run_paths "$2"
    api=$(api_id)
    docker exec -u 0 "$api" rm -f /tmp/private-load-manifest.json /app/tmp/private-load-manifest.json /tmp/capacity-fixture.json /tmp/capacity-preflight.json /tmp/capacity-queue-before.json /tmp/capacity-pgbouncer.jsonl /tmp/capacity-backend.json /tmp/capacity-durability.json
    rm -rf "$private"
    ;;
  *)
    echo "usage: $0 {prepare|deploy|preflight|observe|stop-observers|gate|audit|rollback|cleanup} ..." >&2
    exit 2
    ;;
esac
