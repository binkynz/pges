#!/usr/bin/env bash
# Runs the tests listed in .github/stress/tests.txt over and over for a fixed time, against a PostgreSQL 18
# container with few CPUs, several at once. Prints a table of runs and failures per test and exits 1 if any
# run failed. If the server stops (killed, out of memory, Docker gone), the runs stop, the runs that failed
# because of it are not counted as test failures, and it exits 1 saying the server was lost. docs/testing.md
# (Stress) describes it.
#
# Settings (environment):
#   STRESS_BUDGET_SECS      time to keep starting runs (default 2700)
#   STRESS_JOBS             runs at once (default: the number of CPUs)
#   STRESS_PG_CPUS          docker --cpus for the server (default 1)
#   STRESS_PG_CPUSET        docker --cpuset-cpus for the server, to keep it on given CPUs (default: unset)
#   STRESS_CPU_HOGS         busy loops started next to the runs (default 0)
#   STRESS_RUN_TIMEOUT_SECS a run still going after this is killed and counted as timed out (default 600)
#   STRESS_FILTER           extended regex; only tests whose label matches run (default: all)
#   STRESS_LIST             test list (default .github/stress/tests.txt)
#   STRESS_OUT              output directory (default target/stress); emptied first
#   STRESS_PG_IMAGE         server image (default postgres:18)
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root"

budget=${STRESS_BUDGET_SECS:-2700}
jobs=${STRESS_JOBS:-$(nproc)}
pg_cpus=${STRESS_PG_CPUS:-1}
pg_cpuset=${STRESS_PG_CPUSET:-}
hogs=${STRESS_CPU_HOGS:-0}
run_timeout=${STRESS_RUN_TIMEOUT_SECS:-600}
filter=${STRESS_FILTER:-}
list=${STRESS_LIST:-.github/stress/tests.txt}
out=${STRESS_OUT:-target/stress}
image=${STRESS_PG_IMAGE:-postgres:18}

rm -rf "$out"
mkdir -p "$out/results" "$out/failures" "$out/runs"
out=$(cd "$out" && pwd)

container=pges-stress-$$-$RANDOM
hog_pids=()

cleanup() {
  for pid in "${hog_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  if docker inspect "$container" >/dev/null 2>&1; then
    docker logs "$container" >"$out/postgres.log" 2>&1 || true
    docker inspect -f '{{json .State}}' "$container" >"$out/server-state.json" 2>&1 || true
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "::group::build"
metadata=$(cargo metadata --no-deps --format-version 1)
cargo test --workspace --all-features --no-run --message-format=json-render-diagnostics >"$out/build.json"
# package <tab> target ("lib" or the test file's name) <tab> executable <tab> package directory
jq -r --argjson metadata "$metadata" '
  ($metadata.packages | map({key: .manifest_path, value: .name}) | from_entries) as $names
  | select(.reason == "compiler-artifact" and .profile.test and .executable != null)
  | [$names[.manifest_path], (if .target.kind[0] == "lib" then "lib" else .target.name end), .executable,
     (.manifest_path | sub("/Cargo.toml$"; ""))]
  | @tsv' "$out/build.json" | sort -u >"$out/executables.tsv"
echo "::endgroup::"

# Each unit is one run of one test: label <tab> executable <tab> exact test name <tab> package directory (the
# working directory cargo test would give it). A test of weight n is n units.
units=$out/units.tsv
: >"$units"
missing=0
while read -r package target test weight rest; do
  [[ -z $package || $package == \#* ]] && continue
  weight=${weight:-1}
  if [[ -n $rest || ! $weight =~ ^[1-9][0-9]*$ ]]; then
    echo "$list: expected <package> <target> <test> [weight]: $package $target $test $weight $rest" >&2
    missing=1
    continue
  fi
  IFS=$'\t' read -r exe dir < <(awk -F'\t' -v p="$package" -v t="$target" '$1 == p && $2 == t { print $3 "\t" $4 }' \
    "$out/executables.tsv") || true
  if [[ -z ${exe:-} ]]; then
    echo "$list: no test target $target in package $package" >&2
    missing=1
    continue
  fi
  listed=$("$exe" --list --format terse 2>/dev/null | sed -n 's/: test$//p' | sort)
  ignored=$("$exe" --list --ignored --format terse 2>/dev/null | sed -n 's/: test$//p' | sort)
  runnable=$(comm -23 <(echo "$listed") <(echo "$ignored") | sed '/^$/d')
  if [[ $test == '*' ]]; then
    names=$runnable
  else
    names=$(grep -Fx -- "$test" <<<"$runnable" || true)
  fi
  if [[ -z $names ]]; then
    echo "$list: no test $test in $package/$target" >&2
    missing=1
    continue
  fi
  while read -r name; do
    for _ in $(seq "$weight"); do
      printf '%s/%s/%s\t%s\t%s\t%s\n' "$package" "$target" "$name" "$exe" "$name" "$dir" >>"$units"
    done
  done <<<"$names"
done <"$list"
if ((missing)); then
  echo "fix $list: it names tests that do not exist" >&2
  exit 1
fi
if [[ -n $filter ]]; then
  awk -F'\t' -v f="$filter" '$1 ~ f' "$units" >"$units.filtered"
  mv "$units.filtered" "$units"
fi
# Spread a weighted test's copies over the round.
shuf "$units" -o "$units"
unit_count=$(wc -l <"$units")
if ((unit_count == 0)); then
  echo "no test matches STRESS_FILTER=$filter" >&2
  exit 1
fi
echo "$(cut -f1 "$units" | sort -u | wc -l) tests in a round of $unit_count runs, $jobs at once, ${budget}s, server on $pg_cpus CPUs, $hogs busy loops"

echo "::group::server"
# The settings of SERVER_ARGS in crates/pges-testing/src/lib.rs, more connections for the parallel runs, a
# short deadlock_timeout so a deadlock fails its run sooner, and lock waits logged.
docker run -d --name "$container" --cpus "$pg_cpus" ${pg_cpuset:+--cpuset-cpus "$pg_cpuset"} -p 127.0.0.1::5432 -e POSTGRES_PASSWORD=postgres \
  "$image" \
  -c wal_level=logical -c max_replication_slots=64 -c max_wal_senders=64 -c max_connections=600 \
  -c fsync=off -c synchronous_commit=off -c full_page_writes=off \
  -c deadlock_timeout=50ms -c log_lock_waits=on \
  -c "log_line_prefix=%m [%p] %d %a: " >/dev/null
for _ in $(seq 120); do
  docker exec "$container" pg_isready -q -h 127.0.0.1 -U postgres && break
  sleep 1
done
docker exec "$container" pg_isready -h 127.0.0.1 -U postgres
port=$(docker port "$container" 5432/tcp | head -n1 | sed 's/.*://')
export PGES_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:$port/postgres
echo "::endgroup::"

for _ in $(seq "$hogs"); do
  bash -c 'while :; do :; done' &
  hog_pids+=($!)
done

export RUST_BACKTRACE=1
deadline=$(($(date +%s) + budget))
lost=$out/server-lost

# True while the server's container runs and accepts connections. A server starved of CPU can be slow to
# answer, so it gets several tries before it counts as gone.
server_up() {
  local _
  for _ in $(seq 10); do
    [[ $(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null) == true ]] || return 1
    docker exec "$container" pg_isready -q -h 127.0.0.1 -U postgres -t 5 2>/dev/null && return 0
    sleep 1
  done
  return 1
}

# Worker w goes round the units from its own starting point, so the units run evenly and different pairs
# of tests overlap.
worker() {
  # A background subshell runs the EXIT trap too; only the main shell may remove the server.
  trap - EXIT
  local w=$1 i=$(( $1 * unit_count / jobs )) n=0
  local label exe name dir log started ended status code
  while (($(date +%s) < deadline)) && [[ ! -e $lost ]]; do
    IFS=$'\t' read -r label exe name dir < <(sed -n "$((i % unit_count + 1))p" "$units")
    i=$((i + 1))
    n=$((n + 1))
    log=$out/runs/$w-$n.log
    started=$(date +%s)
    code=0
    (cd "$dir" && exec timeout -k 30 "$run_timeout" "$exe" --exact "$name" --nocapture --test-threads 1) \
      >"$log" 2>&1 || code=$?
    ended=$(date +%s)
    case $code in
      0) status=pass ;;
      124 | 137) status=timeout ;;
      *) status=fail ;;
    esac
    # A run that failed because the server went away says nothing about the test: count it apart and stop.
    if [[ $status != pass ]] && ! server_up; then
      status=server-lost
      [[ -e $lost ]] || date -u +%FT%TZ >"$lost"
    fi
    if [[ $status == pass ]]; then
      rm -f "$log"
    else
      local kept
      kept=$out/failures/$(date -u -d "@$started" +%H%M%S)-$w-$n-${label//\//_}-$status.log
      { echo "# $label: $status (exit $code), $(date -u -d "@$started" +%FT%TZ) to $(date -u -d "@$ended" +%FT%TZ)"
        cat "$log"; } >"$kept"
      rm -f "$log"
      echo "$status: $label after $((ended - started))s, log $kept"
    fi
    printf '%s\t%s\t%s\n' "$label" "$status" "$((ended - started))" >>"$out/results/$w.tsv"
    [[ $status != server-lost ]] || break
  done
}

worker_pids=()
for w in $(seq 0 $((jobs - 1))); do
  worker "$w" &
  worker_pids+=($!)
done
for pid in "${worker_pids[@]}"; do wait "$pid"; done

cleanup
trap - EXIT

deadlocks=$(grep -c 'deadlock detected' "$out/postgres.log" || true)
lost_runs=$(cat "$out"/results/*.tsv 2>/dev/null | awk -F'\t' '$2 == "server-lost"' | wc -l)
cat "$out"/results/*.tsv 2>/dev/null | awk -F'\t' '
  $2 == "server-lost" { next }
  { runs[$1]++; secs[$1] += $3; if ($3 > max[$1]) max[$1] = $3
    if ($2 == "fail") failed[$1]++; if ($2 == "timeout") timed[$1]++ }
  END { for (t in runs) printf "%s\t%d\t%d\t%d\t%.1f\t%d\n", t, runs[t], failed[t], timed[t], secs[t] / runs[t], max[t] }' \
  | sort -t$'\t' -k3,3nr -k4,4nr -k1,1 >"$out/summary.tsv"
total_runs=$(awk -F'\t' '{ s += $2 } END { print s + 0 }' "$out/summary.tsv")
total_failed=$(awk -F'\t' '{ s += $3 + $4 } END { print s + 0 }' "$out/summary.tsv")

{
  echo "| test | runs | failed | timed out | mean s | max s |"
  echo "|---|---:|---:|---:|---:|---:|"
  awk -F'\t' '{ printf "| `%s` | %d | %d | %d | %s | %d |\n", $1, $2, $3, $4, $5, $6 }' "$out/summary.tsv"
  echo
  echo "$total_runs runs, $total_failed failed or timed out; server on $pg_cpus CPUs, $jobs at once," \
    "$hogs busy loops; $deadlocks \`deadlock detected\` lines in the server log."
  if [[ -e $lost ]]; then
    echo
    echo "**The PostgreSQL server stopped at $(cat "$lost")** (state in \`server-state.json\`); the runs stopped" \
      "there. $lost_runs runs that failed with it are not counted above (their logs end in \`-server-lost.log\`)."
  fi
} >"$out/summary.md"
column -t -s$'\t' -N test,runs,failed,timeout,mean_s,max_s "$out/summary.tsv" 2>/dev/null || cat "$out/summary.tsv"
sed -n '/^[0-9]* runs, /,$p' "$out/summary.md"
if [[ -n ${GITHUB_STEP_SUMMARY:-} ]]; then
  { echo "## Stress: ${STRESS_PROFILE:-local}"; echo; cat "$out/summary.md"; } >>"$GITHUB_STEP_SUMMARY"
fi

status=0
if [[ -e $lost ]]; then
  echo "::error::the PostgreSQL server stopped at $(cat "$lost"), not a test failure; the stress run is incomplete" >&2
  status=1
fi
if ((total_failed > 0)); then
  echo "::error::$total_failed of $total_runs stress runs failed; logs in $out/failures" >&2
  status=1
elif ((total_runs == 0)); then
  echo "::error::no run finished" >&2
  status=1
fi
exit "$status"
