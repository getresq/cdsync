#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/run-cdc-benchmark.sh --pg-url URL --publication NAME [options]

Options:
  --runner-cmd CMD          Optional command to run CDSync while the workload runs.
  --table-prefix PREFIX    Source table prefix. Default: cdsync_bench
  --tables N               Number of source tables. Default: 16
  --rows N                 Seed rows per table. Default: 5000
  --rounds N               Workload rounds. Default: 10
  --hot-keys N             Number of hot primary keys per table. Default: 100
  --insert-batch N         Inserted rows per table per round. Default: 500
  --schema-every N         Add one nullable column every N rounds. 0 disables. Default: 0
  --truncate-every N       Truncate and refill table 0 every N rounds. 0 disables. Default: 0
  --summary-path PATH      JSON summary output. Default: /tmp/cdsync-cdc-benchmark-summary.json

The script creates/drops public tables named ${prefix}_${idx}, creates the
publication, optionally starts the runner command, applies a mixed CDC
workload, and writes workload-side timing data. Use CDSync admin/progress data
and BigQuery summaries alongside this output for end-to-end throughput.
USAGE
}

pg_url=""
publication=""
runner_cmd=""
table_prefix="cdsync_bench"
table_count=16
rows_per_table=5000
rounds=10
hot_keys=100
insert_batch=500
schema_every=0
truncate_every=0
summary_path="/tmp/cdsync-cdc-benchmark-summary.json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg-url) pg_url="${2:?}"; shift 2 ;;
    --publication) publication="${2:?}"; shift 2 ;;
    --runner-cmd) runner_cmd="${2:?}"; shift 2 ;;
    --table-prefix) table_prefix="${2:?}"; shift 2 ;;
    --tables) table_count="${2:?}"; shift 2 ;;
    --rows) rows_per_table="${2:?}"; shift 2 ;;
    --rounds) rounds="${2:?}"; shift 2 ;;
    --hot-keys) hot_keys="${2:?}"; shift 2 ;;
    --insert-batch) insert_batch="${2:?}"; shift 2 ;;
    --schema-every) schema_every="${2:?}"; shift 2 ;;
    --truncate-every) truncate_every="${2:?}"; shift 2 ;;
    --summary-path) summary_path="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$pg_url" || -z "$publication" ]]; then
  usage >&2
  exit 2
fi

for value in "$table_count" "$rows_per_table" "$rounds" "$hot_keys" "$insert_batch" "$schema_every" "$truncate_every"; do
  if ! [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "numeric options must be unsigned integers; got $value" >&2
    exit 2
  fi
done
for identifier in "$table_prefix" "$publication"; do
  if ! [[ "$identifier" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "table prefix and publication must be safe SQL identifiers; got $identifier" >&2
    exit 2
  fi
done

runner_pid=""
cleanup() {
  if [[ -n "$runner_pid" ]] && kill -0 "$runner_pid" >/dev/null 2>&1; then
    kill -TERM "$runner_pid" >/dev/null 2>&1 || true
    wait "$runner_pid" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

setup_sql="$(mktemp)"
workload_sql="$(mktemp)"
cat >"$setup_sql" <<SQL
\set ON_ERROR_STOP on
\set table_prefix '$table_prefix'
\set publication '$publication'
\set table_count $table_count
\set rows_per_table $rows_per_table
\set hot_keys $hot_keys

do \$\$
declare
  idx int;
  table_name text;
  table_list text := '';
begin
  execute format('drop publication if exists %I', :'publication');
  for idx in 0..(:'table_count'::int - 1) loop
    table_name := format('%s_%s', :'table_prefix', idx);
    execute format('drop table if exists public.%I', table_name);
    execute format(
      'create table public.%I (
        id bigint primary key,
        payload text,
        hot_key int not null,
        status text not null,
        updated_at timestamptz not null default now(),
        deleted_at timestamptz
      )',
      table_name
    );
    execute format('alter table public.%I replica identity full', table_name);
    execute format(
      'insert into public.%I (id, payload, hot_key, status)
       select gs, concat(%L, gs), (gs %% %s)::int, %L
       from generate_series(1, %s) as gs',
      table_name,
      'seed-',
      greatest(:'hot_keys'::int, 1),
      'seed',
      :'rows_per_table'::int
    );
    table_list := table_list || case when table_list = '' then '' else ', ' end || format('public.%I', table_name);
  end loop;
  execute format('create publication %I for table %s', :'publication', table_list);
end \$\$;
SQL

cat >"$workload_sql" <<SQL
\set ON_ERROR_STOP on
\set table_prefix '$table_prefix'
\set publication '$publication'
\set table_count $table_count
\set rows_per_table $rows_per_table
\set rounds $rounds
\set hot_keys $hot_keys
\set insert_batch $insert_batch
\set schema_every $schema_every
\set truncate_every $truncate_every
\set summary_path '$summary_path'

select clock_timestamp() as workload_started_at \gset
SQL

for ((round_idx = 1; round_idx <= rounds; round_idx++)); do
  for ((idx = 0; idx < table_count; idx++)); do
    table_name="${table_prefix}_${idx}"
    base_id=$((rows_per_table + ((round_idx - 1) * table_count + idx) * insert_batch))
    hot_mod=$((hot_keys > 0 ? hot_keys : 1))
    soft_delete_end=$((hot_keys + (insert_batch < 50 ? insert_batch : 50)))
    cat >>"$workload_sql" <<SQL
update public."$table_name"
set payload = concat(payload, '-hot-r$round_idx'), status = 'hot', updated_at = now()
where id between 1 and $hot_keys;

insert into public."$table_name" (id, payload, hot_key, status)
select $base_id + gs, concat('insert-r$round_idx-', gs), (gs % $hot_mod)::int, 'inserted'
from generate_series(1, $insert_batch) as gs;

update public."$table_name"
set deleted_at = now(), status = 'soft_deleted', updated_at = now()
where id > $hot_keys and id <= $soft_delete_end;

SQL
    if ((schema_every > 0 && round_idx % schema_every == 0)); then
      cat >>"$workload_sql" <<SQL
alter table public."$table_name" add column if not exists "bench_col_$round_idx" text;

SQL
    fi
  done

  if ((truncate_every > 0 && round_idx % truncate_every == 0)); then
    table_name="${table_prefix}_0"
    hot_mod=$((hot_keys > 0 ? hot_keys : 1))
    cat >>"$workload_sql" <<SQL
truncate table public."$table_name";

insert into public."$table_name" (id, payload, hot_key, status)
select gs, concat('refill-r$round_idx-', gs), (gs % $hot_mod)::int, 'refill'
from generate_series(1, $rows_per_table) as gs;

SQL
  fi
done

cat >>"$workload_sql" <<SQL

select clock_timestamp() as workload_finished_at \gset

\copy (
  select jsonb_pretty(jsonb_build_object(
    'table_prefix', :'table_prefix',
    'publication', :'publication',
    'table_count', :'table_count'::int,
    'rows_per_table', :'rows_per_table'::int,
    'rounds', :'rounds'::int,
    'hot_keys', :'hot_keys'::int,
    'insert_batch', :'insert_batch'::int,
    'schema_every', :'schema_every'::int,
    'truncate_every', :'truncate_every'::int,
    'started_at', :'workload_started_at',
    'finished_at', :'workload_finished_at',
    'duration_seconds', extract(epoch from (:'workload_finished_at'::timestamptz - :'workload_started_at'::timestamptz)),
    'estimated_update_events', :'table_count'::int * :'rounds'::int * :'hot_keys'::int,
    'estimated_insert_events', :'table_count'::int * :'rounds'::int * :'insert_batch'::int,
    'estimated_soft_delete_events', :'table_count'::int * :'rounds'::int * least(:'insert_batch'::int, 50)
  ))
) to :'summary_path';
SQL

echo "Preparing benchmark workload tables and publication..."
psql "$pg_url" -f "$setup_sql" >/dev/null

if [[ -n "$runner_cmd" ]]; then
  echo "Starting runner command: $runner_cmd"
  bash -lc "$runner_cmd" &
  runner_pid="$!"
  sleep 5
  if ! kill -0 "$runner_pid" >/dev/null 2>&1; then
    wait "$runner_pid" || true
    echo "runner command exited before workload started" >&2
    exit 1
  fi
fi

echo "Applying benchmark workload..."
psql "$pg_url" -f "$workload_sql" >/dev/null

if [[ -n "$runner_pid" ]] && ! kill -0 "$runner_pid" >/dev/null 2>&1; then
  wait "$runner_pid" || true
  echo "runner command exited before workload completed" >&2
  exit 1
fi

echo "Benchmark workload complete. Summary:"
cat "$summary_path"
