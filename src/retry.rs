use anyhow::Error;
use etl::error::EtlError;
use etl::workers::policy::{RetryDirective, build_error_handling_policy};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

const SYNC_RETRY_INITIAL_BACKOFF_FLOOR_MS: u64 = 16_000;
const SYNC_RETRY_MAX_BACKOFF_MS: u64 = 1_024_000;
const SYNC_RETRY_JITTER_PERCENT: i64 = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncRetryClass {
    Backpressure,
    Transient,
    Permanent,
}

fn classify_etl_error(error: &EtlError) -> SyncRetryClass {
    match build_error_handling_policy(error).retry_directive() {
        RetryDirective::Timed => SyncRetryClass::Transient,
        RetryDirective::Manual | RetryDirective::NoRetry => SyncRetryClass::Permanent,
    }
}

pub fn classify_sync_retry(error: &Error) -> SyncRetryClass {
    for cause in error.chain() {
        if let Some(etl_error) = cause.downcast_ref::<EtlError>() {
            return classify_etl_error(etl_error);
        }
    }

    let message = format!("{error:#}").to_ascii_lowercase();

    if message.contains("quota exceeded") && message.contains("dml jobs writing to a table") {
        return SyncRetryClass::Backpressure;
    }

    if message.contains("schema change detected")
        || message.contains("incompatible schema change detected")
        || message.contains("trigger a manual table resync")
        || message.contains("postgres.publication is required when cdc is enabled")
        || (message.contains("publication") && message.contains("does not exist"))
        || (message.contains("publication") && message.contains("not found"))
    {
        return SyncRetryClass::Permanent;
    }

    SyncRetryClass::Transient
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
        let schema_err = anyhow::anyhow!(
            "schema change detected for public.accounts; trigger a manual table resync"
        );
        assert_eq!(classify_sync_retry(&schema_err), SyncRetryClass::Permanent);

        let publication_err = anyhow::anyhow!("publication cdsync_app_pub does not exist");
        assert_eq!(
            classify_sync_retry(&publication_err),
            SyncRetryClass::Permanent
        );
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
