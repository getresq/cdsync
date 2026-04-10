#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/run-e2e-suite.sh <suite>

Suites:
  emulator   Run the local emulator-backed e2e tests.
  real-bq    Run the real BigQuery e2e tests.
  all        Run both emulator and real-BQ suites explicitly.
  <target>   Run one explicit e2e target by name.

Examples:
  scripts/run-e2e-suite.sh emulator
  scripts/run-e2e-suite.sh real-bq
  scripts/run-e2e-suite.sh e2e_postgres_cdc
EOF
}

require_real_bq_env() {
  local missing=()
  [[ "${CDSYNC_RUN_REAL_BQ_TESTS:-}" == "1" ]] || missing+=("CDSYNC_RUN_REAL_BQ_TESTS=1")
  [[ -n "${CDSYNC_E2E_PG_URL:-}" ]] || missing+=("CDSYNC_E2E_PG_URL")
  [[ -n "${CDSYNC_REAL_BQ_PROJECT:-}" ]] || missing+=("CDSYNC_REAL_BQ_PROJECT")
  [[ -n "${CDSYNC_REAL_BQ_DATASET:-}" ]] || missing+=("CDSYNC_REAL_BQ_DATASET")
  [[ -n "${CDSYNC_REAL_BQ_LOCATION:-}" ]] || missing+=("CDSYNC_REAL_BQ_LOCATION")
  [[ -n "${CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET:-}" ]] || missing+=("CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET")
  [[ -n "${CDSYNC_REAL_BQ_KEY_PATH:-}" ]] || missing+=("CDSYNC_REAL_BQ_KEY_PATH")

  if [[ "${#missing[@]}" -gt 0 ]]; then
    echo "Missing required real BigQuery test prerequisites:" >&2
    printf '  - %s\n' "${missing[@]}" >&2
    exit 1
  fi
}

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 1
fi

case "$1" in
  emulator)
    tests=(
      e2e_postgres_bigquery
      e2e_postgres_cdc
      e2e_postgres_follow_config_changes
      e2e_postgres_schema_changes
      e2e_postgres_soft_deletes
      e2e_runner_graceful_shutdown
    )
    ;;
  real-bq)
    require_real_bq_env
    tests=(
      e2e_postgres_bigquery_real
      e2e_postgres_bigquery_real_cdc
      e2e_postgres_follow_config_changes_real
    )
    ;;
  all)
    require_real_bq_env
    tests=(
      e2e_postgres_bigquery
      e2e_postgres_bigquery_real
      e2e_postgres_bigquery_real_cdc
      e2e_postgres_cdc
      e2e_postgres_follow_config_changes
      e2e_postgres_follow_config_changes_real
      e2e_postgres_schema_changes
      e2e_postgres_soft_deletes
      e2e_runner_graceful_shutdown
    )
    ;;
  *)
    case "$1" in
      *_real|*_real_cdc)
        require_real_bq_env
        ;;
    esac
    tests=("$1")
    ;;
esac

for target in "${tests[@]}"; do
  if [[ "$target" == e2e_postgres_bigquery_real || "$target" == e2e_postgres_bigquery_real_cdc || "$target" == e2e_postgres_follow_config_changes_real ]]; then
    current_feature="real-bq-tests"
  else
    current_feature="integration-tests"
  fi
  echo "==> cargo test --features ${current_feature} --test ${target} -- --nocapture"
  cargo test --features "${current_feature}" --test "${target}" -- --nocapture
done
