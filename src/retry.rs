use anyhow::Error;
use etl::error::{ErrorKind, EtlError};
use etl::workers::policy::{RetryDirective, build_error_handling_policy};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::error::Error as StdError;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::time::Duration;

const SYNC_RETRY_INITIAL_BACKOFF_FLOOR_MS: u64 = 16_000;
const SYNC_RETRY_MAX_BACKOFF_MS: u64 = 1_024_000;
const SYNC_RETRY_JITTER_PERCENT: i64 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncRetryClass {
    Backpressure,
    Transient,
    Permanent,
}

impl SyncRetryClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Backpressure => "backpressure",
            Self::Transient => "transient",
            Self::Permanent => "permanent",
        }
    }
}

impl FromStr for SyncRetryClass {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "backpressure" => Ok(Self::Backpressure),
            "transient" => Ok(Self::Transient),
            "permanent" => Ok(Self::Permanent),
            other => anyhow::bail!("unknown retry class {}", other),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorReasonCode {
    SchemaBlocked,
    PublicationBlocked,
    SnapshotBlocked,
    SnapshotRetryScheduled,
    BigqueryDmlQuota,
    LockContention,
    LastRunFailed,
}

impl ErrorReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SchemaBlocked => "schema_blocked",
            Self::PublicationBlocked => "publication_blocked",
            Self::SnapshotBlocked => "snapshot_blocked",
            Self::SnapshotRetryScheduled => "snapshot_retry_scheduled",
            Self::BigqueryDmlQuota => "bigquery_dml_quota",
            Self::LockContention => "lock_contention",
            Self::LastRunFailed => "last_run_failed",
        }
    }
}

impl FromStr for ErrorReasonCode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "schema_blocked" => Ok(Self::SchemaBlocked),
            "publication_blocked" => Ok(Self::PublicationBlocked),
            "snapshot_blocked" => Ok(Self::SnapshotBlocked),
            "snapshot_retry_scheduled" => Ok(Self::SnapshotRetryScheduled),
            "bigquery_dml_quota" => Ok(Self::BigqueryDmlQuota),
            "lock_contention" => Ok(Self::LockContention),
            "last_run_failed" => Ok(Self::LastRunFailed),
            other => anyhow::bail!("unknown error reason code {}", other),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CdcSyncPolicyError {
    CdcDisabled,
    NoTablesConfigured,
    PublicationRequired,
    PublicationMissing {
        publication: String,
    },
    SchemaChangeDetected {
        table: String,
    },
    IncompatibleSchemaChange {
        table: String,
    },
    SnapshotBlocked {
        tables: Vec<String>,
    },
    SnapshotProgressCheckpointMismatch,
    TableNotInPublication {
        table: String,
        publication: String,
    },
    PublicationFilterMissing {
        publication: String,
        table: String,
    },
    PublicationFilterMismatch {
        publication: String,
        table: String,
        expected: String,
        actual: String,
    },
    WalLevelNotLogical {
        wal_level: String,
    },
}

impl CdcSyncPolicyError {
    pub fn retry_class(&self) -> SyncRetryClass {
        SyncRetryClass::Permanent
    }

    pub fn reason_code(&self) -> ErrorReasonCode {
        match self {
            Self::PublicationRequired | Self::PublicationMissing { .. } => {
                ErrorReasonCode::PublicationBlocked
            }
            Self::TableNotInPublication { .. }
            | Self::PublicationFilterMissing { .. }
            | Self::PublicationFilterMismatch { .. } => ErrorReasonCode::PublicationBlocked,
            Self::SchemaChangeDetected { .. } | Self::IncompatibleSchemaChange { .. } => {
                ErrorReasonCode::SchemaBlocked
            }
            Self::SnapshotBlocked { .. } | Self::SnapshotProgressCheckpointMismatch => {
                ErrorReasonCode::SnapshotBlocked
            }
            Self::CdcDisabled | Self::NoTablesConfigured | Self::WalLevelNotLogical { .. } => {
                ErrorReasonCode::LastRunFailed
            }
        }
    }
}

impl fmt::Display for CdcSyncPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CdcDisabled => write!(f, "CDC is disabled for Postgres source"),
            Self::NoTablesConfigured => write!(f, "no postgres tables configured for CDC"),
            Self::PublicationRequired => {
                write!(f, "postgres.publication is required when CDC is enabled")
            }
            Self::PublicationMissing { publication } => {
                write!(f, "publication '{}' does not exist", publication)
            }
            Self::SchemaChangeDetected { table } => write!(
                f,
                "schema change detected for {}; set schema_changes=auto for additive changes or trigger a manual table resync",
                table
            ),
            Self::IncompatibleSchemaChange { table } => write!(
                f,
                "incompatible schema change detected for {}; trigger a manual table resync",
                table
            ),
            Self::SnapshotBlocked { tables } => {
                write!(f, "snapshot blocked for table(s): {}", tables.join(", "))
            }
            Self::SnapshotProgressCheckpointMismatch => {
                write!(
                    f,
                    "snapshot progress checkpoints have mismatched start LSNs"
                )
            }
            Self::TableNotInPublication { table, publication } => {
                write!(
                    f,
                    "table {} not found in publication {}",
                    table, publication
                )
            }
            Self::PublicationFilterMissing { publication, table } => write!(
                f,
                "publication {} missing row filter for table {}",
                publication, table
            ),
            Self::PublicationFilterMismatch {
                publication,
                table,
                expected,
                actual,
            } => write!(
                f,
                "publication {} row filter mismatch for {} (expected `{}`, got `{}`)",
                publication, table, expected, actual
            ),
            Self::WalLevelNotLogical { wal_level } => {
                write!(
                    f,
                    "wal_level must be logical for CDC (found '{}')",
                    wal_level
                )
            }
        }
    }
}

impl StdError for CdcSyncPolicyError {}

#[derive(Debug, Clone)]
pub struct ConnectionLockError {
    connection_id: String,
}

impl ConnectionLockError {
    pub fn new(connection_id: impl Into<String>) -> Self {
        Self {
            connection_id: connection_id.into(),
        }
    }

    pub fn reason_code(&self) -> ErrorReasonCode {
        ErrorReasonCode::LockContention
    }
}

impl fmt::Display for ConnectionLockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "connection {} is already locked", self.connection_id)
    }
}

impl StdError for ConnectionLockError {}

fn classify_etl_error(error: &EtlError) -> Option<SyncRetryClass> {
    match error.kind() {
        ErrorKind::DestinationError | ErrorKind::SourceError | ErrorKind::Unknown => None,
        _ => Some(match build_error_handling_policy(error).retry_directive() {
            RetryDirective::Timed => SyncRetryClass::Transient,
            RetryDirective::Manual | RetryDirective::NoRetry => SyncRetryClass::Permanent,
        }),
    }
}

pub fn classify_sync_retry(error: &Error) -> SyncRetryClass {
    for cause in error.chain() {
        if let Some(policy_error) = cause.downcast_ref::<CdcSyncPolicyError>() {
            return policy_error.retry_class();
        }
        if let Some(lock_error) = cause.downcast_ref::<ConnectionLockError>() {
            let _ = lock_error;
            return SyncRetryClass::Permanent;
        }
        if let Some(etl_error) = cause.downcast_ref::<EtlError>()
            && let Some(classification) = classify_etl_error(etl_error)
        {
            return classification;
        }
    }

    let message = format!("{error:#}").to_ascii_lowercase();

    if message.contains("quota exceeded") && message.contains("dml jobs writing to a table") {
        return SyncRetryClass::Backpressure;
    }

    SyncRetryClass::Transient
}

pub fn classify_error_reason(error: &Error) -> ErrorReasonCode {
    for cause in error.chain() {
        if let Some(policy_error) = cause.downcast_ref::<CdcSyncPolicyError>() {
            return policy_error.reason_code();
        }
        if let Some(lock_error) = cause.downcast_ref::<ConnectionLockError>() {
            return lock_error.reason_code();
        }
    }

    if classify_sync_retry(error) == SyncRetryClass::Backpressure {
        ErrorReasonCode::BigqueryDmlQuota
    } else {
        ErrorReasonCode::LastRunFailed
    }
}

pub fn table_runtime_reason_for_retry_class(
    retry_class: SyncRetryClass,
    blocked: bool,
) -> ErrorReasonCode {
    match (retry_class, blocked) {
        (SyncRetryClass::Backpressure, _) => ErrorReasonCode::BigqueryDmlQuota,
        (_, true) => ErrorReasonCode::SnapshotBlocked,
        (_, false) => ErrorReasonCode::SnapshotRetryScheduled,
    }
}

pub fn compute_sync_retry_backoff(retry_key: &str, attempt: u32, configured_ms: u64) -> Duration {
    let initial_ms = configured_ms.max(SYNC_RETRY_INITIAL_BACKOFF_FLOOR_MS);
    let exponent = attempt.saturating_sub(1).min(20);
    let base_ms = (u128::from(initial_ms) * (1_u128 << exponent))
        .min(u128::from(SYNC_RETRY_MAX_BACKOFF_MS)) as u64;

    let mut hasher = DefaultHasher::new();
    retry_key.hash(&mut hasher);
    attempt.hash(&mut hasher);
    let jitter_bucket = (hasher.finish() % ((SYNC_RETRY_JITTER_PERCENT as u64 * 2) + 1)) as i64
        - SYNC_RETRY_JITTER_PERCENT;
    let jittered_ms = i128::from(base_ms) + (i128::from(base_ms) * i128::from(jitter_bucket) / 100);
    let bounded_ms = jittered_ms
        .max(i128::from(initial_ms))
        .min(i128::from(SYNC_RETRY_MAX_BACKOFF_MS)) as u64;

    Duration::from_millis(bounded_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_sync_retry_marks_bigquery_dml_quota_as_backpressure() {
        let err = anyhow::anyhow!(
            "Quota exceeded: Your table exceeded quota for total number of dml jobs writing to a table, pending + running"
        );
        assert_eq!(classify_sync_retry(&err), SyncRetryClass::Backpressure);
    }

    #[test]
    fn classify_sync_retry_marks_schema_and_publication_errors_permanent() {
        let schema_err = anyhow::Error::new(CdcSyncPolicyError::SchemaChangeDetected {
            table: "public.accounts".to_string(),
        });
        assert_eq!(classify_sync_retry(&schema_err), SyncRetryClass::Permanent);

        let publication_err = anyhow::Error::new(CdcSyncPolicyError::PublicationMissing {
            publication: "cdsync_app_pub".to_string(),
        });
        assert_eq!(
            classify_sync_retry(&publication_err),
            SyncRetryClass::Permanent
        );

        let blocked_err = anyhow::Error::new(CdcSyncPolicyError::SnapshotBlocked {
            tables: vec!["public.accounts".to_string()],
        });
        assert_eq!(classify_sync_retry(&blocked_err), SyncRetryClass::Permanent);
    }

    #[test]
    fn classify_sync_retry_does_not_short_circuit_generic_destination_errors() {
        let err = anyhow::Error::new(etl::etl_error!(
            ErrorKind::DestinationError,
            "generic destination failure",
            "Quota exceeded: Your table exceeded quota for total number of dml jobs writing to a table, pending + running"
        ));
        assert_eq!(classify_sync_retry(&err), SyncRetryClass::Backpressure);

        let transient = anyhow::Error::new(etl::etl_error!(
            ErrorKind::DestinationError,
            "generic destination failure",
            source: std::io::Error::other("connection reset by peer")
        ));
        assert_eq!(classify_sync_retry(&transient), SyncRetryClass::Transient);
    }

    #[test]
    fn classify_sync_retry_defaults_other_errors_to_transient() {
        let err = anyhow::anyhow!("connection reset by peer");
        assert_eq!(classify_sync_retry(&err), SyncRetryClass::Transient);
    }

    #[test]
    fn compute_sync_retry_backoff_uses_floor_and_cap_without_wrapping() {
        let first = compute_sync_retry_backoff("app:postgres_cdc", 1, 1_000);
        assert!(first >= Duration::from_secs(16));
        assert!(first <= Duration::from_secs(16) * 64 / 50);

        let later = compute_sync_retry_backoff("app:postgres_cdc", 30, 1_000);
        assert!(later <= Duration::from_secs(1024));
        assert!(later >= Duration::from_secs(16));
    }

    #[test]
    fn compute_sync_retry_backoff_honors_higher_configured_floor() {
        let backoff = compute_sync_retry_backoff("app:postgres_table:public.accounts", 1, 30_000);
        assert!(backoff >= Duration::from_secs(30));
        assert!(backoff <= Duration::from_secs(36));
    }
}
