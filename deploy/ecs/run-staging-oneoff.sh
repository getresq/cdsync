#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <image-uri> <db-password-ssm-arn> <gcp-key-b64-ssm-arn>"
  exit 1
fi

IMAGE_URI="$1"
DB_PASSWORD_SSM_ARN="$2"
GCP_KEY_B64_SSM_ARN="$3"

TMP_TASKDEF="$(mktemp)"
trap 'rm -f "$TMP_TASKDEF"' EXIT

OVERRIDES_JSON="$(mktemp)"
trap 'rm -f "$TMP_TASKDEF" "$OVERRIDES_JSON"' EXIT

sed \
  -e "s#__IMAGE_URI__#${IMAGE_URI}#g" \
  -e "s#__DB_PASSWORD_SSM_ARN__#${DB_PASSWORD_SSM_ARN}#g" \
  -e "s#__GCP_KEY_B64_SSM_ARN__#${GCP_KEY_B64_SSM_ARN}#g" \
  /Users/mazdak/Code/cdsync/deploy/ecs/cdsync-staging-oneoff-taskdef.template.json > "$TMP_TASKDEF"

TASK_DEF_ARN="$(
  aws ecs register-task-definition \
    --region us-east-1 \
    --cli-input-json "file://${TMP_TASKDEF}" \
    --query 'taskDefinition.taskDefinitionArn' \
    --output text
)"

echo "registered task definition: ${TASK_DEF_ARN}"

declare -a override_names=(
  CDSYNC_DB_HOST
  CDSYNC_DB_NAME
  CDSYNC_DB_USER
  CDSYNC_STATE_SCHEMA
  CDSYNC_STATS_SCHEMA
  CDSYNC_CONNECTION_ID
  CDSYNC_OBSERVABILITY_SERVICE_NAME
  CDSYNC_BIGQUERY_PROJECT_ID
  CDSYNC_DESTINATION_DATASET
  CDSYNC_BIGQUERY_LOCATION
  CDSYNC_BATCH_LOAD_BUCKET
  CDSYNC_BATCH_LOAD_PREFIX
  CDSYNC_PUBLICATION
  CDSYNC_CDC_PIPELINE_ID
  CDSYNC_RUN_MIGRATE
)

override_entries=""
for name in "${override_names[@]}"; do
  value="${!name-}"
  if [[ -n "${value}" ]]; then
    escaped_value="$(printf '%s' "${value}" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')"
    if [[ -n "${override_entries}" ]]; then
      override_entries+=","
    fi
    override_entries+="{\"name\":\"${name}\",\"value\":${escaped_value}}"
  fi
done

printf '{"containerOverrides":[{"name":"cdsync","environment":[%s]}]}\n' "${override_entries}" > "${OVERRIDES_JSON}"

aws ecs run-task \
  --region us-east-1 \
  --cluster cluster-staging \
  --launch-type FARGATE \
  --task-definition "${TASK_DEF_ARN}" \
  --network-configuration 'awsvpcConfiguration={subnets=[subnet-0847f8d9415013ba2,subnet-0a790db993d728821],securityGroups=[sg-06d149ded90962461],assignPublicIp=DISABLED}' \
  --overrides "file://${OVERRIDES_JSON}" \
  --query 'tasks[0].taskArn' \
  --output text
