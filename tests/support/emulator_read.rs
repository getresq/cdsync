use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;

pub async fn fetch_table_fields(
    client: &Client,
    base_url: &str,
    project: &str,
    dataset: &str,
    table: &str,
) -> Result<Vec<String>> {
    let url = format!(
        "{}/projects/{}/datasets/{}/tables/{}",
        base_url, project, dataset, table
    );
    let payload: Value = client.get(url).send().await?.json().await?;
    let fields = payload
        .pointer("/schema/fields")
        .and_then(|v| v.as_array())
        .context("missing schema.fields in emulator response")?;
    fields
        .iter()
        .map(|field| {
            field
                .get("name")
                .and_then(|v| v.as_str())
                .map(std::string::ToString::to_string)
                .context("schema field missing name")
        })
        .collect()
}

pub async fn fetch_table_rows(
    client: &Client,
    base_url: &str,
    project: &str,
    dataset: &str,
    table: &str,
) -> Result<Vec<Vec<Value>>> {
    let url = format!(
        "{}/projects/{}/datasets/{}/tables/{}/data",
        base_url, project, dataset, table
    );
    let payload: Value = client.get(url).send().await?.json().await?;
    let rows = payload
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        let cells = row
            .get("f")
            .and_then(|v| v.as_array())
            .context("row missing f array")?;
        let values = cells
            .iter()
            .map(|cell| cell.get("v").cloned().unwrap_or(Value::Null))
            .collect();
        output.push(values);
    }
    Ok(output)
}

pub fn map_rows(fields: &[String], rows: Vec<Vec<Value>>) -> Result<Vec<HashMap<String, Value>>> {
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        if row.len() != fields.len() {
            anyhow::bail!(
                "row length {} does not match field length {}",
                row.len(),
                fields.len()
            );
        }
        let mut map = HashMap::new();
        for (idx, field) in fields.iter().enumerate() {
            map.insert(field.clone(), row[idx].clone());
        }
        output.push(map);
    }
    Ok(output)
}

pub fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}
