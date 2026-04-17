use crate::config::DynamoDbConfig;
use crate::destinations::{ChangeApplier, Destination, WriteMode};
use crate::runner::ShutdownSignal;
use crate::sources::contract::records_to_dataframe;
use crate::state::{ConnectionState, DynamoDbFollowState, DynamoDbShardState, StateHandle};
use crate::types::{
    ChangeOperation, ColumnSchema, DataType, META_SOURCE_EVENT_AT, META_SOURCE_EVENT_ID,
    MetadataColumns, SourceChangeBatch, SourceChangeRecord, SourceEntity, SourceKind, SourceRecord,
    TableCheckpoint, TableSchema, destination_table_name,
};
use anyhow::{Context, Result};
use async_compression::tokio::bufread::GzipDecoder;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::types::{
    ApproximateCreationDateTimePrecision, ExportFormat, ExportStatus, KeySchemaElement, KeyType,
    PointInTimeRecoveryStatus,
};
use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_kinesis::types::{Shard, ShardIteratorType};
use aws_sdk_s3::Client as S3Client;
use aws_smithy_types::DateTime as AwsDateTime;
use aws_types::region::Region;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value};
use tokio::io::{AsyncBufReadExt, BufReader};

const FOLLOW_IDLE_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);
const EXPORT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const FOLLOW_RECORD_LIMIT: i32 = 1_000;

pub struct DynamoDbSource {
    config: DynamoDbConfig,
    metadata: MetadataColumns,
    dynamodb: DynamoClient,
    kinesis: KinesisClient,
    s3: S3Client,
}

pub struct DynamoDbSyncRequest<'a> {
    pub dest: &'a dyn Destination,
    pub change_applier: &'a dyn ChangeApplier,
    pub state: &'a mut ConnectionState,
    pub state_handle: Option<StateHandle>,
    pub mode: crate::types::SyncMode,
    pub dry_run: bool,
    pub follow: bool,
    pub default_batch_size: usize,
    pub shutdown: Option<ShutdownSignal>,
}

#[derive(Clone)]
struct DynamoDbResolvedTable {
    entity: SourceEntity,
    table_arn: String,
    stream_arn: String,
}

struct DecodedChangeBatch {
    batch: SourceChangeBatch,
    last_sequence_number: Option<String>,
    reached_cutoff: bool,
}

impl DynamoDbSource {
    pub async fn new(config: DynamoDbConfig, metadata: MetadataColumns) -> Result<Self> {
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .load()
            .await;
        Ok(Self {
            config,
            metadata,
            dynamodb: DynamoClient::new(&sdk_config),
            kinesis: KinesisClient::new(&sdk_config),
            s3: S3Client::new(&sdk_config),
        })
    }

    pub async fn validate(&self) -> Result<SourceEntity> {
        Ok(self.resolve_table().await?.entity)
    }

    pub async fn sync(&self, request: DynamoDbSyncRequest<'_>) -> Result<()> {
        let DynamoDbSyncRequest {
            dest,
            change_applier,
            state,
            state_handle,
            mode,
            dry_run,
            follow,
            default_batch_size,
            shutdown,
        } = request;

        let resolved = self.resolve_table().await?;
        let table_name = self.config.table_name.clone();
        let should_run_snapshot = matches!(mode, crate::types::SyncMode::Full)
            || state
                .dynamodb
                .get(&table_name)
                .and_then(|checkpoint| checkpoint.last_synced_at)
                .is_none();

        if should_run_snapshot {
            let cutover_time = Utc::now();
            let follow_state = DynamoDbFollowState {
                table_name: table_name.clone(),
                stream_arn: Some(resolved.stream_arn.clone()),
                cutover_time: Some(cutover_time),
                shard_checkpoints: std::collections::HashMap::new(),
                updated_at: Some(Utc::now()),
            };
            state.dynamodb_follow = Some(follow_state.clone());
            if let Some(state_handle) = &state_handle {
                state_handle
                    .save_dynamodb_follow_state(&follow_state)
                    .await
                    .context("saving dynamodb follow state before snapshot")?;
            }

            self.run_snapshot(
                &resolved,
                dest,
                cutover_time,
                dry_run,
                default_batch_size,
                shutdown.clone(),
            )
            .await?;

            let checkpoint = TableCheckpoint {
                last_synced_at: Some(cutover_time),
                schema_primary_key: Some(resolved.entity.primary_key.clone()),
                schema_snapshot: Some(
                    resolved
                        .entity
                        .schema
                        .columns
                        .iter()
                        .map(|column| crate::types::SchemaFieldSnapshot {
                            name: column.name.clone(),
                            data_type: column.data_type.clone(),
                            nullable: column.nullable,
                        })
                        .collect(),
                ),
                ..Default::default()
            };
            state
                .dynamodb
                .insert(table_name.clone(), checkpoint.clone());
            if let Some(state_handle) = &state_handle {
                state_handle
                    .save_dynamodb_checkpoint(&table_name, &checkpoint)
                    .await
                    .context("saving dynamodb snapshot checkpoint")?;
            }
        }

        self.follow_changes(
            &resolved,
            change_applier,
            state,
            state_handle.as_ref(),
            follow,
            shutdown,
        )
        .await
    }

    async fn resolve_table(&self) -> Result<DynamoDbResolvedTable> {
        let describe = self
            .dynamodb
            .describe_table()
            .table_name(&self.config.table_name)
            .send()
            .await
            .with_context(|| format!("describing DynamoDB table {}", self.config.table_name))?;
        let table = describe
            .table
            .context("dynamodb table description missing table")?;
        let table_arn = table
            .table_arn
            .context("dynamodb table missing table arn")?;
        let key_schema = table.key_schema.unwrap_or_default();
        let primary_key = self.resolve_primary_key(&key_schema)?;

        let backups = self
            .dynamodb
            .describe_continuous_backups()
            .table_name(&self.config.table_name)
            .send()
            .await
            .with_context(|| format!("describing PITR state for {}", self.config.table_name))?;
        let backup_description = backups
            .continuous_backups_description
            .context("continuous backups description missing")?;
        let pitr = backup_description
            .point_in_time_recovery_description
            .context("point in time recovery description missing")?;
        if pitr.point_in_time_recovery_status != Some(PointInTimeRecoveryStatus::Enabled) {
            anyhow::bail!(
                "dynamodb PITR is not enabled for {}; CDSync requires PITR exports",
                self.config.table_name
            );
        }

        let stream_arn = self.resolve_stream_arn().await?;
        let destination_name = destination_table_name(&self.config.table_name);
        let entity = SourceEntity {
            kind: SourceKind::DynamoDb,
            source_name: self.config.table_name.clone(),
            destination_name: destination_name.clone(),
            schema: self.entity_schema(&destination_name, &primary_key),
            primary_key,
        };

        Ok(DynamoDbResolvedTable {
            entity,
            table_arn,
            stream_arn,
        })
    }

    async fn resolve_stream_arn(&self) -> Result<String> {
        let destinations = self
            .dynamodb
            .describe_kinesis_streaming_destination()
            .table_name(&self.config.table_name)
            .send()
            .await
            .with_context(|| {
                format!(
                    "describing kinesis streaming destination for {}",
                    self.config.table_name
                )
            })?;
        let configured = destinations
            .kinesis_data_stream_destinations
            .unwrap_or_default();
        let desired_stream_name = self.config.kinesis_stream_name.as_deref();
        let desired_stream_arn = self.config.kinesis_stream_arn.as_deref();
        let destination = configured
            .into_iter()
            .find(|destination| {
                let arn = destination.stream_arn.as_deref();
                let matches_arn = desired_stream_arn.is_some_and(|value| arn == Some(value));
                let matches_name = desired_stream_name.zip(arn).is_some_and(|(name, arn)| {
                    arn.rsplit('/').next().is_some_and(|tail| tail == name)
                });
                matches_arn || matches_name
            })
            .context("configured kinesis destination not found on dynamodb table")?;
        if destination.approximate_creation_date_time_precision
            != Some(ApproximateCreationDateTimePrecision::Microsecond)
        {
            anyhow::bail!(
                "kinesis destination for {} must use MICROSECOND precision",
                self.config.table_name
            );
        }
        destination
            .stream_arn
            .context("kinesis destination missing stream arn")
    }

    async fn run_snapshot(
        &self,
        resolved: &DynamoDbResolvedTable,
        dest: &dyn Destination,
        cutover_time: DateTime<Utc>,
        dry_run: bool,
        default_batch_size: usize,
        shutdown: Option<ShutdownSignal>,
    ) -> Result<()> {
        let prefix = self.export_prefix(cutover_time);
        let export = self
            .dynamodb
            .export_table_to_point_in_time()
            .table_arn(&resolved.table_arn)
            .s3_bucket(&self.config.export_bucket)
            .s3_prefix(&prefix)
            .export_format(ExportFormat::DynamodbJson)
            .export_time(to_aws_datetime(cutover_time))
            .send()
            .await
            .with_context(|| format!("starting PITR export for {}", self.config.table_name))?;
        let export_arn = export
            .export_description
            .and_then(|description| description.export_arn)
            .context("export response missing export arn")?;

        loop {
            if shutdown_requested(&shutdown) {
                return Ok(());
            }
            let status = self
                .dynamodb
                .describe_export()
                .export_arn(&export_arn)
                .send()
                .await
                .context("describing dynamodb export")?;
            let description = status
                .export_description
                .context("describe_export missing export description")?;
            match description.export_status {
                Some(ExportStatus::Completed) => break,
                Some(ExportStatus::Failed) => {
                    anyhow::bail!(
                        "dynamodb export {} did not complete successfully",
                        export_arn
                    );
                }
                _ => tokio::time::sleep(EXPORT_POLL_INTERVAL).await,
            }
        }

        let schema = &resolved.entity.schema;
        dest.ensure_table(schema).await?;
        if !dry_run {
            dest.truncate_table(&resolved.entity.destination_name)
                .await?;
        }

        let data_keys = self.list_export_data_keys(&prefix).await?;
        let mut pending_records = Vec::with_capacity(default_batch_size.max(1));
        for key in data_keys {
            if shutdown_requested(&shutdown) {
                return Ok(());
            }
            let object = self
                .s3
                .get_object()
                .bucket(&self.config.export_bucket)
                .key(&key)
                .send()
                .await
                .with_context(|| {
                    format!(
                        "downloading export object s3://{}/{}",
                        self.config.export_bucket, key
                    )
                })?;
            let reader = BufReader::new(object.body.into_async_read());
            let decoder = GzipDecoder::new(reader);
            let mut lines = BufReader::new(decoder).lines();
            while let Some(line) = lines.next_line().await? {
                let item = decode_export_item(&line)?;
                pending_records.push(self.snapshot_record_from_item(&item, cutover_time));
                if pending_records.len() >= default_batch_size.max(1) {
                    self.flush_snapshot_batch(dest, &resolved.entity, &mut pending_records)
                        .await?;
                }
            }
        }
        self.flush_snapshot_batch(dest, &resolved.entity, &mut pending_records)
            .await
    }

    async fn flush_snapshot_batch(
        &self,
        dest: &dyn Destination,
        entity: &SourceEntity,
        records: &mut Vec<SourceRecord>,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let chunk = crate::types::SourceSnapshotChunk {
            entity: entity.clone(),
            records: records.clone(),
            checkpoint: None,
        };
        let frame = records_to_dataframe(&chunk.entity.schema, &chunk.records, &self.metadata)?;
        dest.write_batch(
            &chunk.entity.destination_name,
            &chunk.entity.schema,
            &frame,
            WriteMode::Upsert,
            Some(chunk.entity.primary_key.as_str()),
        )
        .await?;
        records.clear();
        Ok(())
    }

    async fn list_export_data_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let mut continuation_token = None;
        let mut keys = Vec::new();

        loop {
            let response = self
                .s3
                .list_objects_v2()
                .bucket(&self.config.export_bucket)
                .prefix(prefix)
                .set_continuation_token(continuation_token)
                .send()
                .await
                .with_context(|| {
                    format!(
                        "listing export objects under s3://{}/{}",
                        self.config.export_bucket, prefix
                    )
                })?;
            for object in response.contents.unwrap_or_default() {
                if let Some(key) = object.key
                    && key.contains("/data/")
                    && key.ends_with(".json.gz")
                {
                    keys.push(key);
                }
            }
            if !response.is_truncated.unwrap_or(false) {
                break;
            }
            continuation_token = response.next_continuation_token;
        }

        keys.sort();
        if keys.is_empty() {
            anyhow::bail!(
                "no export data files found under s3://{}/{}",
                self.config.export_bucket,
                prefix
            );
        }
        Ok(keys)
    }

    async fn follow_changes(
        &self,
        resolved: &DynamoDbResolvedTable,
        change_applier: &dyn ChangeApplier,
        state: &mut ConnectionState,
        state_handle: Option<&StateHandle>,
        follow: bool,
        shutdown: Option<ShutdownSignal>,
    ) -> Result<()> {
        let mut follow_state = state
            .dynamodb_follow
            .clone()
            .context("dynamodb follow state missing after snapshot bootstrap")?;
        if let Some(stream_arn) = &follow_state.stream_arn
            && stream_arn != &resolved.stream_arn
        {
            anyhow::bail!(
                "dynamodb stream changed from {} to {}; rebuild from PITR is required",
                stream_arn,
                resolved.stream_arn
            );
        }
        follow_state.stream_arn = Some(resolved.stream_arn.clone());
        let catch_up_cutoff = (!follow).then(Utc::now);

        loop {
            if shutdown_requested(&shutdown) {
                break;
            }

            let shards = self.list_shards(&resolved.stream_arn).await?;
            for shard in shards {
                if shutdown_requested(&shutdown) {
                    break;
                }
                let shard_id = shard.shard_id;
                let mut iterator = self
                    .shard_iterator(
                        &resolved.stream_arn,
                        &shard_id,
                        follow_state.cutover_time,
                        follow_state
                            .shard_checkpoints
                            .get(&shard_id)
                            .and_then(|state| state.sequence_number.clone()),
                    )
                    .await?;
                while let Some(current_iterator) = iterator.take() {
                    if shutdown_requested(&shutdown) {
                        break;
                    }
                    let response = self
                        .kinesis
                        .get_records()
                        .shard_iterator(current_iterator)
                        .limit(FOLLOW_RECORD_LIMIT)
                        .send()
                        .await
                        .with_context(|| format!("reading kinesis shard {}", shard_id))?;
                    let records = response.records;
                    iterator = response.next_shard_iterator;
                    if records.is_empty() {
                        break;
                    }
                    let batch = self.change_batch_from_records(
                        &resolved.entity,
                        &shard_id,
                        records,
                        catch_up_cutoff,
                    )?;
                    if !batch.batch.records.is_empty() {
                        change_applier.apply_change_batch(&batch.batch).await?;
                        if let Some(last_record) = batch.batch.records.last() {
                            let table_checkpoint = state
                                .dynamodb
                                .entry(self.config.table_name.clone())
                                .or_default();
                            table_checkpoint.last_synced_at = Some(last_record.event_time);
                            if let Some(state_handle) = state_handle {
                                state_handle
                                    .save_dynamodb_checkpoint(
                                        &self.config.table_name,
                                        table_checkpoint,
                                    )
                                    .await
                                    .context("saving dynamodb follow checkpoint")?;
                            }
                        }
                    }
                    if let Some(last_sequence) = batch.last_sequence_number {
                        follow_state.shard_checkpoints.insert(
                            shard_id.clone(),
                            DynamoDbShardState {
                                sequence_number: Some(last_sequence),
                                updated_at: Some(Utc::now()),
                            },
                        );
                    }
                    follow_state.updated_at = Some(Utc::now());
                    state.dynamodb_follow = Some(follow_state.clone());
                    if let Some(state_handle) = state_handle {
                        state_handle
                            .save_dynamodb_follow_state(&follow_state)
                            .await
                            .context("saving dynamodb shard checkpoint state")?;
                    }
                    if batch.reached_cutoff {
                        break;
                    }
                }
            }

            if !follow {
                break;
            }
            tokio::time::sleep(FOLLOW_IDLE_POLL_INTERVAL).await;
        }

        state.dynamodb_follow = Some(follow_state);
        Ok(())
    }

    async fn list_shards(&self, stream_arn: &str) -> Result<Vec<Shard>> {
        let mut shards = Vec::new();
        let mut next_token = None;
        loop {
            let response = self
                .kinesis
                .list_shards()
                .stream_arn(stream_arn)
                .set_next_token(next_token)
                .send()
                .await
                .with_context(|| format!("listing kinesis shards for {}", stream_arn))?;
            shards.extend(response.shards.unwrap_or_default());
            let Some(token) = response.next_token else {
                break;
            };
            next_token = Some(token);
        }
        if shards.is_empty() {
            anyhow::bail!("kinesis stream {} has no readable shards", stream_arn);
        }
        Ok(shards)
    }

    async fn shard_iterator(
        &self,
        stream_arn: &str,
        shard_id: &str,
        cutover_time: Option<DateTime<Utc>>,
        last_sequence_number: Option<String>,
    ) -> Result<Option<String>> {
        let mut builder = self
            .kinesis
            .get_shard_iterator()
            .stream_arn(stream_arn)
            .shard_id(shard_id);
        if let Some(sequence_number) = last_sequence_number {
            builder = builder
                .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
                .starting_sequence_number(sequence_number);
        } else if let Some(cutover_time) = cutover_time {
            builder = builder
                .shard_iterator_type(ShardIteratorType::AtTimestamp)
                .timestamp(to_aws_datetime(cutover_time));
        } else {
            builder = builder.shard_iterator_type(ShardIteratorType::TrimHorizon);
        }

        let response = builder
            .send()
            .await
            .with_context(|| format!("getting shard iterator for {}", shard_id))?;
        Ok(response.shard_iterator)
    }

    fn change_batch_from_records(
        &self,
        entity: &SourceEntity,
        shard_id: &str,
        records: Vec<aws_sdk_kinesis::types::Record>,
        catch_up_cutoff: Option<DateTime<Utc>>,
    ) -> Result<DecodedChangeBatch> {
        let mut change_records = Vec::new();
        let mut last_sequence_number = None;
        let mut reached_cutoff = false;
        for record in records {
            let sequence_number = record.sequence_number.clone();
            let raw = std::str::from_utf8(record.data.as_ref())
                .context("kinesis record was not utf-8")?;
            let payload: Value =
                serde_json::from_str(raw).context("decoding dynamodb kinesis record JSON")?;
            let event_name = payload
                .get("eventName")
                .and_then(Value::as_str)
                .context("dynamodb kinesis record missing eventName")?;
            let dynamodb = payload
                .get("dynamodb")
                .and_then(Value::as_object)
                .context("dynamodb kinesis record missing dynamodb payload")?;
            let event_time = dynamodb
                .get("ApproximateCreationDateTime")
                .and_then(Value::as_f64)
                .map(timestamp_seconds_to_utc)
                .or_else(|| {
                    payload
                        .get("approximateCreationDateTime")
                        .and_then(Value::as_f64)
                        .map(timestamp_seconds_to_utc)
                })
                .unwrap_or_else(Utc::now);
            let arrival_time = record
                .approximate_arrival_timestamp()
                .and_then(aws_datetime_to_utc)
                .unwrap_or(event_time);
            if catch_up_cutoff.is_some_and(|cutoff| arrival_time > cutoff) {
                reached_cutoff = true;
                break;
            }
            let event_id = format!("{shard_id}:{sequence_number}");

            let source_item = match event_name {
                "REMOVE" => dynamodb.get("OldImage").or_else(|| dynamodb.get("Keys")),
                _ => dynamodb.get("NewImage").or_else(|| dynamodb.get("Keys")),
            }
            .and_then(Value::as_object)
            .context("dynamodb kinesis record missing image payload")?;
            let item = decode_attribute_map(source_item)?;
            let deleted_at = matches!(event_name, "REMOVE").then_some(event_time);
            let record = SourceRecord {
                values: self.record_values_from_item(&item, Some(event_time), Some(&event_id)),
                synced_at: event_time,
                deleted_at,
            };
            change_records.push(SourceChangeRecord {
                operation: if matches!(event_name, "REMOVE") {
                    ChangeOperation::Delete
                } else {
                    ChangeOperation::Upsert
                },
                event_time,
                event_id,
                record,
            });
            last_sequence_number = Some(sequence_number);
        }
        Ok(DecodedChangeBatch {
            batch: SourceChangeBatch {
                entity: entity.clone(),
                records: change_records,
                checkpoint: None,
            },
            last_sequence_number,
            reached_cutoff,
        })
    }

    fn snapshot_record_from_item(
        &self,
        item: &Map<String, Value>,
        synced_at: DateTime<Utc>,
    ) -> SourceRecord {
        SourceRecord {
            values: self.record_values_from_item(item, None, None),
            synced_at,
            deleted_at: None,
        }
    }

    fn record_values_from_item(
        &self,
        item: &Map<String, Value>,
        event_time: Option<DateTime<Utc>>,
        event_id: Option<&str>,
    ) -> std::collections::BTreeMap<String, Value> {
        let mut values = std::collections::BTreeMap::new();
        for attribute in &self.config.attributes {
            values.insert(
                attribute.name.clone(),
                item.get(&attribute.name).cloned().unwrap_or(Value::Null),
            );
        }
        values.insert(
            self.config.raw_item_column_name().to_string(),
            Value::Object(item.clone()),
        );
        values.insert(
            META_SOURCE_EVENT_AT.to_string(),
            event_time.map_or(Value::Null, |value| {
                Value::String(value.to_rfc3339_opts(SecondsFormat::Micros, true))
            }),
        );
        values.insert(
            META_SOURCE_EVENT_ID.to_string(),
            event_id.map_or(Value::Null, |value| Value::String(value.to_string())),
        );
        values
    }

    fn entity_schema(&self, destination_name: &str, primary_key: &str) -> TableSchema {
        let mut columns = self
            .config
            .attributes
            .iter()
            .map(crate::config::DynamoDbAttributeConfig::to_column_schema)
            .collect::<Vec<_>>();
        columns.push(ColumnSchema {
            name: self.config.raw_item_column_name().to_string(),
            data_type: DataType::Json,
            nullable: true,
        });
        columns.push(ColumnSchema {
            name: META_SOURCE_EVENT_AT.to_string(),
            data_type: DataType::Timestamp,
            nullable: true,
        });
        columns.push(ColumnSchema {
            name: META_SOURCE_EVENT_ID.to_string(),
            data_type: DataType::String,
            nullable: true,
        });
        TableSchema {
            name: destination_name.to_string(),
            columns,
            primary_key: Some(primary_key.to_string()),
        }
    }

    fn configured_primary_key(&self) -> &str {
        self.config
            .key_attributes
            .first()
            .map_or("id", String::as_str)
    }

    fn resolve_primary_key(&self, key_schema: &[KeySchemaElement]) -> Result<String> {
        if key_schema.len() != 1 {
            anyhow::bail!(
                "dynamodb table {} uses a composite primary key; CDSync currently supports exactly one hash key per connection",
                self.config.table_name
            );
        }
        let hash_key = key_schema
            .iter()
            .find(|element| matches!(element.key_type(), KeyType::Hash))
            .map(|element| element.attribute_name().to_string())
            .context("dynamodb table key schema is missing a HASH key")?;
        let configured_primary_key = self.configured_primary_key();
        if hash_key != configured_primary_key {
            anyhow::bail!(
                "dynamodb key attribute mismatch for {}: configured {}, actual {}",
                self.config.table_name,
                configured_primary_key,
                hash_key
            );
        }
        Ok(hash_key)
    }

    fn export_prefix(&self, cutover_time: DateTime<Utc>) -> String {
        match &self.config.export_prefix {
            Some(prefix) if !prefix.trim().is_empty() => {
                format!(
                    "{}/{}",
                    prefix.trim_end_matches('/'),
                    cutover_time.format("%Y%m%dT%H%M%S")
                )
            }
            _ => format!(
                "cdsync/dynamodb/{}/{}",
                self.config.table_name,
                cutover_time.format("%Y%m%dT%H%M%S")
            ),
        }
    }
}

fn shutdown_requested(shutdown: &Option<ShutdownSignal>) -> bool {
    shutdown.as_ref().is_some_and(ShutdownSignal::is_shutdown)
}

fn timestamp_seconds_to_utc(seconds: f64) -> DateTime<Utc> {
    let whole = seconds.trunc() as i64;
    let nanos = ((seconds.fract() * 1_000_000_000_f64).round() as u32).min(999_999_999);
    DateTime::<Utc>::from_timestamp(whole, nanos).unwrap_or_else(Utc::now)
}

fn to_aws_datetime(value: DateTime<Utc>) -> AwsDateTime {
    AwsDateTime::from_secs(value.timestamp())
}

fn aws_datetime_to_utc(value: &AwsDateTime) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(value.secs(), value.subsec_nanos())
}

fn decode_export_item(line: &str) -> Result<Map<String, Value>> {
    let record: Value = serde_json::from_str(line).context("parsing export line")?;
    let item = record
        .get("Item")
        .and_then(Value::as_object)
        .context("export line missing Item payload")?;
    decode_attribute_map(item)
}

fn decode_attribute_map(map: &Map<String, Value>) -> Result<Map<String, Value>> {
    map.iter()
        .map(|(key, value)| Ok((key.clone(), decode_attribute_value(value)?)))
        .collect()
}

fn decode_attribute_value(value: &Value) -> Result<Value> {
    let object = value
        .as_object()
        .context("dynamodb attribute value must be an object")?;
    if let Some(value) = object.get("S").and_then(Value::as_str) {
        return Ok(Value::String(value.to_string()));
    }
    if let Some(value) = object.get("N").and_then(Value::as_str) {
        return Ok(Value::String(value.to_string()));
    }
    if let Some(value) = object.get("BOOL").and_then(Value::as_bool) {
        return Ok(Value::Bool(value));
    }
    if object.get("NULL").and_then(Value::as_bool).unwrap_or(false) {
        return Ok(Value::Null);
    }
    if let Some(value) = object.get("B").and_then(Value::as_str) {
        return Ok(Value::String(value.to_string()));
    }
    if let Some(values) = object.get("SS").and_then(Value::as_array) {
        return Ok(Value::Array(values.clone()));
    }
    if let Some(values) = object.get("NS").and_then(Value::as_array) {
        let decoded = values.iter().map(decode_number_string).collect::<Vec<_>>();
        return Ok(Value::Array(decoded));
    }
    if let Some(values) = object.get("BS").and_then(Value::as_array) {
        return Ok(Value::Array(values.clone()));
    }
    if let Some(map) = object.get("M").and_then(Value::as_object) {
        return Ok(Value::Object(decode_attribute_map(map)?));
    }
    if let Some(values) = object.get("L").and_then(Value::as_array) {
        return Ok(Value::Array(
            values
                .iter()
                .map(decode_attribute_value)
                .collect::<Result<Vec<_>>>()?,
        ));
    }
    Ok(Value::Null)
}

fn decode_number_string(value: &Value) -> Value {
    let Some(value) = value.as_str() else {
        return Value::Null;
    };
    Value::String(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DynamoDbAttributeConfig, DynamoDbAttributeType};
    use aws_sdk_kinesis::types::Record;
    use aws_smithy_types::Blob;
    use chrono::TimeZone;

    fn test_source() -> DynamoDbSource {
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .test_credentials()
            .load();
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let sdk_config = runtime.block_on(sdk_config);
        DynamoDbSource {
            config: DynamoDbConfig {
                table_name: "BuildTable".to_string(),
                region: "us-east-1".to_string(),
                export_bucket: "bucket".to_string(),
                export_prefix: None,
                kinesis_stream_name: Some("build-stream".to_string()),
                kinesis_stream_arn: None,
                raw_item_column: None,
                key_attributes: vec!["id".to_string()],
                attributes: vec![
                    DynamoDbAttributeConfig {
                        name: "id".to_string(),
                        data_type: DynamoDbAttributeType::String,
                        nullable: Some(false),
                    },
                    DynamoDbAttributeConfig {
                        name: "status".to_string(),
                        data_type: DynamoDbAttributeType::String,
                        nullable: Some(true),
                    },
                ],
            },
            metadata: MetadataColumns::default(),
            dynamodb: DynamoClient::new(&sdk_config),
            kinesis: KinesisClient::new(&sdk_config),
            s3: S3Client::new(&sdk_config),
        }
    }

    #[test]
    fn decode_attribute_value_handles_nested_items() -> anyhow::Result<()> {
        let input = serde_json::json!({
            "M": {
                "id": { "S": "abc" },
                "count": { "N": "4" },
                "enabled": { "BOOL": true },
                "tags": { "L": [{ "S": "one" }, { "S": "two" }] }
            }
        });
        let decoded = decode_attribute_value(&input)?;
        assert_eq!(decoded["id"], Value::String("abc".to_string()));
        assert_eq!(decoded["count"], Value::String("4".to_string()));
        assert_eq!(decoded["enabled"], Value::Bool(true));
        assert_eq!(decoded["tags"][0], Value::String("one".to_string()));
        Ok(())
    }

    #[test]
    fn decode_attribute_value_preserves_exact_number_strings() -> anyhow::Result<()> {
        let input = serde_json::json!({
            "M": {
                "huge": { "N": "123456789012345678901234567890.123456789" },
                "set": { "NS": ["18446744073709551616", "0.1234567890123456789"] }
            }
        });

        let decoded = decode_attribute_value(&input)?;

        assert_eq!(
            decoded["huge"],
            Value::String("123456789012345678901234567890.123456789".to_string())
        );
        assert_eq!(
            decoded["set"],
            Value::Array(vec![
                Value::String("18446744073709551616".to_string()),
                Value::String("0.1234567890123456789".to_string()),
            ])
        );
        Ok(())
    }

    #[test]
    fn record_values_include_raw_item_and_event_metadata() {
        let source = test_source();
        let item = Map::from_iter([
            ("id".to_string(), Value::String("build-1".to_string())),
            ("status".to_string(), Value::String("queued".to_string())),
        ]);
        let values = source.record_values_from_item(&item, Some(Utc::now()), Some("shard-000:123"));
        assert_eq!(values["id"], Value::String("build-1".to_string()));
        assert_eq!(values["status"], Value::String("queued".to_string()));
        assert!(values.contains_key("raw_item_json"));
        assert!(values.contains_key(META_SOURCE_EVENT_AT));
        assert!(values.contains_key(META_SOURCE_EVENT_ID));
    }

    #[test]
    fn entity_schema_uses_destination_table_name() {
        let mut source = test_source();
        source.config.table_name = "orders-prod".to_string();

        let destination_name = destination_table_name(&source.config.table_name);
        let schema = source.entity_schema(&destination_name, "id");

        assert_eq!(schema.name, "orders_prod");
        assert_eq!(schema.primary_key.as_deref(), Some("id"));
    }

    #[test]
    fn resolve_primary_key_rejects_composite_key_tables() {
        let source = test_source();
        let key_schema = vec![
            KeySchemaElement::builder()
                .attribute_name("site_id")
                .key_type(KeyType::Hash)
                .build()
                .expect("hash key"),
            KeySchemaElement::builder()
                .attribute_name("revision_id")
                .key_type(KeyType::Range)
                .build()
                .expect("range key"),
        ];

        let err = source
            .resolve_primary_key(&key_schema)
            .expect_err("composite keys should be rejected");

        assert!(err.to_string().contains("composite primary key"));
    }

    #[test]
    fn resolve_primary_key_rejects_config_mismatch() {
        let mut source = test_source();
        source.config.key_attributes = vec!["site_id".to_string()];
        let key_schema = vec![
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .expect("hash key"),
        ];

        let err = source
            .resolve_primary_key(&key_schema)
            .expect_err("mismatched keys should be rejected");

        assert!(err.to_string().contains("key attribute mismatch"));
    }

    #[test]
    fn change_batch_from_records_stops_at_one_shot_cutoff() -> anyhow::Result<()> {
        let source = test_source();
        let entity = SourceEntity {
            kind: SourceKind::DynamoDb,
            source_name: "BuildTable".to_string(),
            destination_name: "BuildTable".to_string(),
            schema: source.entity_schema("BuildTable", "id"),
            primary_key: "id".to_string(),
        };
        let cutoff = Utc
            .with_ymd_and_hms(2026, 4, 16, 10, 0, 1)
            .single()
            .expect("valid cutoff");
        let cutoff_seconds = cutoff.timestamp();

        let batch = source.change_batch_from_records(
            &entity,
            "shard-000",
            vec![
                kinesis_record("1", cutoff_seconds, "build-1"),
                kinesis_record("2", cutoff_seconds, "build-2"),
                kinesis_record("3", cutoff_seconds + 1, "build-3"),
            ],
            Some(cutoff),
        )?;

        assert_eq!(batch.batch.records.len(), 2);
        assert_eq!(batch.last_sequence_number.as_deref(), Some("2"));
        assert!(batch.reached_cutoff);
        Ok(())
    }

    fn kinesis_record(sequence: &str, arrival_second: i64, item_id: &str) -> Record {
        let payload = serde_json::json!({
            "eventName": "INSERT",
            "dynamodb": {
                "ApproximateCreationDateTime": arrival_second as f64,
                "NewImage": {
                    "id": { "S": item_id },
                    "status": { "S": "queued" }
                }
            }
        });
        Record::builder()
            .sequence_number(sequence)
            .partition_key(item_id)
            .approximate_arrival_timestamp(AwsDateTime::from_secs(arrival_second))
            .data(Blob::new(payload.to_string().into_bytes()))
            .build()
            .expect("record")
    }
}
