use async_trait::async_trait;
use cdsync::config::{SalesforceConfig, SalesforceRateLimitConfig};
use cdsync::destinations::{Destination, WriteMode};
use cdsync::sources::salesforce::{
    ResolvedSalesforceObject, SalesforceSource, SalesforceSyncRequest,
};
use cdsync::types::TableCheckpoint;
use cdsync::types::{SyncMode, TableSchema};
use polars::prelude::DataFrame;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

struct TestDestination;

#[async_trait]
impl Destination for TestDestination {
    async fn ensure_table(&self, _schema: &TableSchema) -> anyhow::Result<()> {
        Ok(())
    }

    async fn truncate_table(&self, _table: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn write_batch(
        &self,
        _table: &str,
        _schema: &TableSchema,
        _frame: &DataFrame,
        _mode: WriteMode,
        _primary_key: Option<&str>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn salesforce_retries_on_rate_limit() -> anyhow::Result<()> {
    let server = MockServer::start().await;

    let auth_response = serde_json::json!({
        "access_token": "token",
        "instance_url": server.uri(),
    });
    Mock::given(method("POST"))
        .and(path("/services/oauth2/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(auth_response))
        .mount(&server)
        .await;

    let describe_response = serde_json::json!({
        "fields": [
            {"name": "Id", "type": "string", "nillable": false},
            {"name": "Name", "type": "string", "nillable": true},
            {"name": "SystemModstamp", "type": "datetime", "nillable": false},
            {"name": "IsDeleted", "type": "boolean", "nillable": false}
        ]
    });
    Mock::given(method("GET"))
        .and(path("/services/data/v59.0/sobjects/Account/describe"))
        .respond_with(ResponseTemplate::new(200).set_body_json(describe_response))
        .mount(&server)
        .await;

    let query_response = serde_json::json!({
        "records": [
            {
                "Id": "001",
                "Name": "Acme",
                "SystemModstamp": "2024-01-01T00:00:00.000Z",
                "IsDeleted": false
            }
        ],
        "nextRecordsUrl": null
    });
    let attempts = Arc::new(AtomicUsize::new(0));
    let responder = RateLimitResponder {
        attempts: attempts.clone(),
        success_body: query_response,
    };
    Mock::given(method("GET"))
        .and(path("/services/data/v59.0/query/"))
        .respond_with(responder)
        .mount(&server)
        .await;

    let config = SalesforceConfig {
        client_id: "client".to_string(),
        client_secret: "secret".to_string(),
        refresh_token: "refresh".to_string(),
        login_url: Some(server.uri()),
        instance_url: None,
        api_version: Some("v59.0".to_string()),
        objects: None,
        object_selection: None,
        polling_interval_seconds: None,
        rate_limit: Some(SalesforceRateLimitConfig {
            max_retries: Some(2),
            backoff_ms: Some(1),
            max_backoff_ms: Some(2),
        }),
    };

    let source = SalesforceSource::new(config)?;
    let object = ResolvedSalesforceObject {
        name: "Account".to_string(),
        primary_key: "Id".to_string(),
        fields: None,
        soft_delete: true,
    };
    let dest = TestDestination;
    source
        .sync_object(SalesforceSyncRequest {
            object: &object,
            dest: &dest,
            checkpoint: TableCheckpoint::default(),
            state_handle: None,
            mode: SyncMode::Incremental,
            dry_run: true,
            stats: None,
        })
        .await?;

    server.verify().await;
    assert!(attempts.load(Ordering::SeqCst) >= 2);
    Ok(())
}

#[derive(Clone)]
struct RateLimitResponder {
    attempts: Arc<AtomicUsize>,
    success_body: serde_json::Value,
}

impl Respond for RateLimitResponder {
    fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            ResponseTemplate::new(429).insert_header("Retry-After", "0")
        } else {
            ResponseTemplate::new(200).set_body_json(self.success_body.clone())
        }
    }
}
