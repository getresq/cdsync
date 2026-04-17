use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::types::{
    ApproximateCreationDateTimePrecision, AttributeDefinition, BillingMode, KeySchemaElement,
    KeyType, ScalarAttributeType,
};
use aws_sdk_kinesis::Client as KinesisClient;
use aws_smithy_types::Blob;
use cdsync::config::{DynamoDbAttributeConfig, DynamoDbAttributeType, DynamoDbConfig};
use cdsync::destinations::{ChangeApplier, Destination, WriteMode};
use cdsync::sources::dynamodb::{DynamoDbSource, DynamoDbSyncRequest};
use cdsync::state::{ConnectionState, DynamoDbFollowState};
use cdsync::types::{
    ChangeOperation, MetadataColumns, SourceChangeBatch, SyncMode, TableCheckpoint, TableSchema,
};
use chrono::{Duration, Utc};
use polars::prelude::DataFrame;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Default)]
struct NoopDestination;

#[async_trait]
impl Destination for NoopDestination {
    async fn ensure_table(&self, _schema: &TableSchema) -> Result<()> {
        Ok(())
    }

    async fn truncate_table(&self, _table: &str) -> Result<()> {
        Ok(())
    }

    async fn write_batch(
        &self,
        _table: &str,
        _schema: &TableSchema,
        _frame: &DataFrame,
        _mode: WriteMode,
        _primary_key: Option<&str>,
    ) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct RecordingChangeApplier {
    batches: Mutex<Vec<SourceChangeBatch>>,
}

#[async_trait]
impl ChangeApplier for RecordingChangeApplier {
    async fn apply_change_batch(&self, batch: &SourceChangeBatch) -> Result<()> {
        self.batches.lock().await.push(batch.clone());
        Ok(())
    }
}

#[tokio::test]
async fn e2e_dynamodb_localstack_follow_consumes_synthetic_kinesis_records() -> Result<()> {
    let Some(endpoint) = std::env::var("CDSYNC_E2E_LOCALSTACK_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(());
    };

    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_types::region::Region::new("us-east-1"))
        .endpoint_url(endpoint.clone())
        .test_credentials()
        .load()
        .await;
    let dynamodb = DynamoClient::new(&sdk_config);
    let kinesis = KinesisClient::new(&sdk_config);

    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("cdsync_e2e_build_{}", &suffix[..8]);
    let stream_name = format!("cdsync-e2e-build-{}", &suffix[..8]);

    kinesis
        .create_stream()
        .stream_name(&stream_name)
        .shard_count(1)
        .send()
        .await
        .with_context(|| format!("creating localstack kinesis stream {stream_name}"))?;
    dynamodb
        .create_table()
        .table_name(&table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()?,
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()?,
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .with_context(|| format!("creating localstack dynamodb table {table_name}"))?;

    let stream = kinesis
        .describe_stream_summary()
        .stream_name(&stream_name)
        .send()
        .await?
        .stream_description_summary
        .context("missing stream summary")?;
    let stream_arn = stream.stream_arn;

    dynamodb
        .enable_kinesis_streaming_destination()
        .table_name(&table_name)
        .stream_arn(&stream_arn)
        .enable_kinesis_streaming_configuration(
            aws_sdk_dynamodb::types::EnableKinesisStreamingConfiguration::builder()
                .approximate_creation_date_time_precision(
                    ApproximateCreationDateTimePrecision::Microsecond,
                )
                .build(),
        )
        .send()
        .await
        .with_context(|| format!("enabling kinesis destination for {table_name}"))?;

    let result = run_follow_case(&kinesis, &table_name, &stream_name, &stream_arn).await;

    let _ = dynamodb.delete_table().table_name(&table_name).send().await;
    let _ = kinesis
        .delete_stream()
        .stream_name(&stream_name)
        .send()
        .await;

    result
}

async fn run_follow_case(
    kinesis: &KinesisClient,
    table_name: &str,
    stream_name: &str,
    stream_arn: &str,
) -> Result<()> {
    // LocalStack gives us real Kinesis shard/iterator behavior, but its DynamoDB
    // PITR export and table-to-Kinesis fidelity are not the release gate. Feed
    // DynamoDB-shaped records directly to Kinesis so this test stays focused on
    // CDSync follow consumption, decoding, and checkpoint state.
    put_dynamodb_kinesis_record(kinesis, stream_name, "1", "INSERT", "build-1", "queued").await?;
    put_dynamodb_kinesis_record(kinesis, stream_name, "2", "MODIFY", "build-1", "succeeded")
        .await?;
    put_dynamodb_remove_record(kinesis, stream_name, "3", "build-2").await?;

    let source = DynamoDbSource::new(
        DynamoDbConfig {
            table_name: table_name.to_string(),
            region: "us-east-1".to_string(),
            export_bucket: "unused-localstack-export-bucket".to_string(),
            export_prefix: None,
            kinesis_stream_name: Some(stream_name.to_string()),
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
        MetadataColumns::default(),
    )
    .await?;
    let dest = NoopDestination;
    let applier = Arc::new(RecordingChangeApplier::default());
    let mut state = ConnectionState::default();
    state.dynamodb.insert(
        table_name.to_string(),
        TableCheckpoint {
            last_synced_at: Some(Utc::now() - Duration::minutes(1)),
            ..Default::default()
        },
    );
    state.dynamodb_follow = Some(DynamoDbFollowState {
        table_name: table_name.to_string(),
        stream_arn: Some(stream_arn.to_string()),
        cutover_time: Some(Utc::now() - Duration::minutes(5)),
        snapshot_in_progress: false,
        shard_count: None,
        shard_checkpoints: Default::default(),
        updated_at: None,
    });

    source
        .sync(DynamoDbSyncRequest {
            dest: &dest,
            change_applier: applier.as_ref(),
            state: &mut state,
            state_handle: None,
            mode: SyncMode::Incremental,
            dry_run: false,
            follow: false,
            default_batch_size: 100,
            shutdown: None,
        })
        .await?;

    let batches = applier.batches.lock().await;
    let records = batches
        .iter()
        .flat_map(|batch| batch.records.iter())
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].operation, ChangeOperation::Upsert);
    assert_eq!(records[1].record.values["status"], "succeeded");
    assert_eq!(records[2].operation, ChangeOperation::Delete);
    drop(batches);

    let follow = state.dynamodb_follow.context("missing follow state")?;
    assert_eq!(follow.shard_count, Some(1));
    assert_eq!(follow.shard_checkpoints.len(), 1);
    assert!(
        follow
            .shard_checkpoints
            .values()
            .all(|shard| shard.sequence_number.is_some())
    );
    assert!(
        state
            .dynamodb
            .get(table_name)
            .and_then(|checkpoint| checkpoint.last_synced_at)
            .is_some()
    );

    Ok(())
}

async fn put_dynamodb_kinesis_record(
    kinesis: &KinesisClient,
    stream_name: &str,
    sequence: &str,
    event_name: &str,
    id: &str,
    status: &str,
) -> Result<()> {
    let payload = serde_json::json!({
        "eventName": event_name,
        "dynamodb": {
            "ApproximateCreationDateTime": Utc::now().timestamp_micros() as f64 / 1_000_000_f64,
            "NewImage": {
                "id": { "S": id },
                "status": { "S": status }
            }
        }
    });
    kinesis
        .put_record()
        .stream_name(stream_name)
        .partition_key(id)
        .data(Blob::new(payload.to_string().into_bytes()))
        .explicit_hash_key(sequence)
        .send()
        .await?;
    Ok(())
}

async fn put_dynamodb_remove_record(
    kinesis: &KinesisClient,
    stream_name: &str,
    sequence: &str,
    id: &str,
) -> Result<()> {
    let payload = serde_json::json!({
        "eventName": "REMOVE",
        "dynamodb": {
            "ApproximateCreationDateTime": Utc::now().timestamp_micros() as f64 / 1_000_000_f64,
            "Keys": {
                "id": { "S": id }
            }
        }
    });
    kinesis
        .put_record()
        .stream_name(stream_name)
        .partition_key(id)
        .data(Blob::new(payload.to_string().into_bytes()))
        .explicit_hash_key(sequence)
        .send()
        .await?;
    Ok(())
}
