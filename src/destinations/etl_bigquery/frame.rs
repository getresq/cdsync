use super::*;

pub(super) async fn build_cdc_frame(
    info: CdcTableInfo,
    rows: Vec<TableRow>,
    synced_at: DateTime<Utc>,
    deleted_at_override: Option<DateTime<Utc>>,
) -> EtlResult<DataFrame> {
    if rows.len() < CDC_FRAME_BUILD_BLOCKING_ROWS {
        return table_rows_to_frame(&info, &rows, synced_at, deleted_at_override);
    }

    task::spawn_blocking(move || table_rows_to_frame(&info, &rows, synced_at, deleted_at_override))
        .await
        .map_err(|err| {
            etl::etl_error!(
                ErrorKind::DestinationError,
                "failed to join CDC frame build task",
                err.to_string()
            )
        })?
}

pub(super) fn compact_table_events(
    info: &CdcTableInfo,
    table_id: TableId,
    events: Vec<Event>,
) -> EtlResult<CdcCommitTableWork> {
    let mut row_actions: HashMap<String, PendingTableRowAction> = HashMap::new();
    let mut truncate = false;

    for event in events {
        match event {
            Event::Insert(insert_event) => {
                if insert_event.table_id != table_id {
                    return Err(etl::etl_error!(
                        ErrorKind::DestinationError,
                        "received mixed-table CDC insert batch"
                    ));
                }
                let key = primary_key_identity(info, &insert_event.table_row)?;
                row_actions.insert(key, PendingTableRowAction::Upsert(insert_event.table_row));
            }
            Event::Update(update_event) => {
                if update_event.table_id != table_id {
                    return Err(etl::etl_error!(
                        ErrorKind::DestinationError,
                        "received mixed-table CDC update batch"
                    ));
                }
                let key = primary_key_identity(info, &update_event.table_row)?;
                row_actions.insert(key, PendingTableRowAction::Upsert(update_event.table_row));
            }
            Event::Delete(delete_event) => {
                if delete_event.table_id != table_id {
                    return Err(etl::etl_error!(
                        ErrorKind::DestinationError,
                        "received mixed-table CDC delete batch"
                    ));
                }
                if !info.soft_delete {
                    continue;
                }
                if let Some((_, old_row)) = delete_event.old_table_row {
                    let key = primary_key_identity(info, &old_row)?;
                    row_actions.insert(key, PendingTableRowAction::Delete(old_row));
                } else {
                    warn!(
                        table = %info.source_name,
                        "skipping delete event without primary key"
                    );
                }
            }
            Event::Truncate(truncate_event) => {
                if !truncate_event
                    .rel_ids
                    .iter()
                    .any(|rel_id| TableId::new(*rel_id) == table_id)
                {
                    return Err(etl::etl_error!(
                        ErrorKind::DestinationError,
                        "received mixed-table CDC truncate batch"
                    ));
                }
                truncate = true;
                row_actions.clear();
            }
            Event::Begin(_) | Event::Commit(_) | Event::Relation(_) | Event::Unsupported => {}
        }
    }

    let mut rows = Vec::new();
    let mut delete_rows = Vec::new();
    for action in row_actions.into_values() {
        match action {
            PendingTableRowAction::Upsert(row) => rows.push(row),
            PendingTableRowAction::Delete(row) => delete_rows.push(row),
        }
    }

    Ok(CdcCommitTableWork {
        table_id,
        rows,
        delete_rows,
        truncate,
    })
}

fn primary_key_identity(info: &CdcTableInfo, row: &TableRow) -> EtlResult<String> {
    let cell = row
        .values()
        .get(info.source_primary_key_index)
        .ok_or_else(|| {
            etl::etl_error!(
                ErrorKind::ConversionError,
                "missing primary key value in CDC row",
                info.primary_key.clone()
            )
        })?;
    cell_identity_key(cell)
}

fn cell_identity_key(cell: &Cell) -> EtlResult<String> {
    let key = match cell {
        Cell::Null => {
            return Err(etl::etl_error!(
                ErrorKind::ConversionError,
                "null primary key value in CDC row"
            ));
        }
        Cell::Bool(value) => value.to_string(),
        Cell::String(value) => value.clone(),
        Cell::I16(value) => value.to_string(),
        Cell::I32(value) => value.to_string(),
        Cell::U32(value) => value.to_string(),
        Cell::I64(value) => value.to_string(),
        Cell::F32(value) => value.to_string(),
        Cell::F64(value) => value.to_string(),
        Cell::Numeric(value) => value.to_string(),
        Cell::Date(value) => value.format("%Y-%m-%d").to_string(),
        Cell::Time(value) => format_time(value),
        Cell::Timestamp(value) => Utc.from_utc_datetime(value).to_rfc3339(),
        Cell::TimestampTz(value) => value.to_rfc3339(),
        Cell::Uuid(value) => value.to_string(),
        Cell::Json(value) => value.to_string(),
        Cell::Bytes(value) => encode_base64(value),
        Cell::Array(array) => serde_json::to_string(&array_to_json(array)).map_err(|err| {
            etl::etl_error!(
                ErrorKind::ConversionError,
                "failed to encode array primary key",
                err.to_string()
            )
        })?,
    };
    Ok(key)
}

pub(super) fn table_rows_to_frame(
    info: &CdcTableInfo,
    rows: &[TableRow],
    synced_at: DateTime<Utc>,
    deleted_at_override: Option<DateTime<Utc>>,
) -> Result<DataFrame, EtlError> {
    let polars_schema = polars_schema_with_metadata(&info.schema, &info.metadata)?;
    let mut output: Vec<PolarsRow> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values: Vec<AnyValue> = Vec::with_capacity(polars_schema.len());
        for (column, source_idx) in info
            .schema
            .columns
            .iter()
            .zip(info.dest_source_indices.iter())
        {
            let cell = row.values().get(*source_idx).unwrap_or(&Cell::Null);
            values.push(cell_to_anyvalue(cell, &column.data_type));
        }

        values.push(AnyValue::StringOwned(PlSmallStr::from(
            synced_at.to_rfc3339(),
        )));

        let deleted_at = if info.soft_delete {
            if let Some(ts) = deleted_at_override {
                Some(ts.to_rfc3339())
            } else {
                derive_deleted_at_from_row(info, row)
            }
        } else {
            None
        };

        match deleted_at {
            Some(ts) => values.push(AnyValue::StringOwned(PlSmallStr::from(ts))),
            None => values.push(AnyValue::Null),
        }

        output.push(PolarsRow::new(values));
    }

    DataFrame::from_rows_and_schema(&output, &polars_schema).map_err(|err| {
        etl::etl_error!(
            ErrorKind::ConversionError,
            "failed to build CDC dataframe",
            err.to_string()
        )
    })
}

pub(super) fn derive_deleted_at_from_row(info: &CdcTableInfo, row: &TableRow) -> Option<String> {
    let idx = info.source_soft_delete_index?;
    let cell = row.values().get(idx)?;
    match cell {
        Cell::Null => None,
        Cell::Bool(value) => {
            if *value {
                Some(Utc::now().to_rfc3339())
            } else {
                None
            }
        }
        Cell::Timestamp(value) => Some(Utc.from_utc_datetime(value).to_rfc3339()),
        Cell::TimestampTz(value) => Some(value.to_rfc3339()),
        Cell::Date(value) => Some(value.format("%Y-%m-%d").to_string()),
        Cell::String(value) => Some(value.clone()),
        _ => None,
    }
}

fn cell_to_anyvalue(cell: &Cell, data_type: &DataType) -> AnyValue<'static> {
    if matches!(data_type, DataType::Interval) {
        return match cell {
            Cell::Null => AnyValue::Null,
            Cell::F32(value) => AnyValue::Float64(*value as f64),
            Cell::F64(value) => AnyValue::Float64(*value),
            _ => AnyValue::Null,
        };
    }

    match cell {
        Cell::Null => AnyValue::Null,
        Cell::Bool(value) => AnyValue::Boolean(*value),
        Cell::String(value) => AnyValue::StringOwned(PlSmallStr::from(value.as_str())),
        Cell::I16(value) => AnyValue::Int64(*value as i64),
        Cell::I32(value) => AnyValue::Int64(*value as i64),
        Cell::U32(value) => AnyValue::Int64(*value as i64),
        Cell::I64(value) => AnyValue::Int64(*value),
        Cell::F32(value) => AnyValue::Float64(*value as f64),
        Cell::F64(value) => AnyValue::Float64(*value),
        Cell::Numeric(value) => AnyValue::StringOwned(PlSmallStr::from(value.to_string())),
        Cell::Date(value) => {
            AnyValue::StringOwned(PlSmallStr::from(value.format("%Y-%m-%d").to_string()))
        }
        Cell::Time(value) => AnyValue::StringOwned(PlSmallStr::from(format_time(value))),
        Cell::Timestamp(value) => {
            AnyValue::StringOwned(PlSmallStr::from(Utc.from_utc_datetime(value).to_rfc3339()))
        }
        Cell::TimestampTz(value) => AnyValue::StringOwned(PlSmallStr::from(value.to_rfc3339())),
        Cell::Uuid(value) => AnyValue::StringOwned(PlSmallStr::from(value.to_string())),
        Cell::Json(value) => AnyValue::StringOwned(PlSmallStr::from(value.to_string())),
        Cell::Bytes(value) => AnyValue::StringOwned(PlSmallStr::from(encode_base64(value))),
        Cell::Array(array) => {
            let json = serde_json::to_string(&array_to_json(array)).unwrap_or_default();
            AnyValue::StringOwned(PlSmallStr::from(json))
        }
    }
}

fn array_to_json(array: &etl::types::ArrayCell) -> serde_json::Value {
    match array {
        etl::types::ArrayCell::Bool(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.map(serde_json::Value::Bool)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::String(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref().map_or(serde_json::Value::Null, |s| {
                        serde_json::Value::String(s.clone())
                    })
                })
                .collect(),
        ),
        etl::types::ArrayCell::I16(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.map(|n| serde_json::Value::Number((n as i64).into()))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::I32(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.map(|n| serde_json::Value::Number((n as i64).into()))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::U32(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.map(|n| serde_json::Value::Number((n as u64).into()))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::I64(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.map(|n| serde_json::Value::Number(n.into()))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::F32(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.and_then(|n| {
                        serde_json::Number::from_f64(n as f64).map(serde_json::Value::Number)
                    })
                    .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::F64(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.and_then(|n| serde_json::Number::from_f64(n).map(serde_json::Value::Number))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::Numeric(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref().map_or(serde_json::Value::Null, |n| {
                        serde_json::Value::String(n.to_string())
                    })
                })
                .collect(),
        ),
        etl::types::ArrayCell::Date(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref().map_or(serde_json::Value::Null, |d| {
                        serde_json::Value::String(d.format("%Y-%m-%d").to_string())
                    })
                })
                .collect(),
        ),
        etl::types::ArrayCell::Time(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref().map_or(serde_json::Value::Null, |t| {
                        serde_json::Value::String(format_time(t))
                    })
                })
                .collect(),
        ),
        etl::types::ArrayCell::Timestamp(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref().map_or(serde_json::Value::Null, |t| {
                        serde_json::Value::String(Utc.from_utc_datetime(t).to_rfc3339())
                    })
                })
                .collect(),
        ),
        etl::types::ArrayCell::TimestampTz(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref().map_or(serde_json::Value::Null, |t| {
                        serde_json::Value::String(t.to_rfc3339())
                    })
                })
                .collect(),
        ),
        etl::types::ArrayCell::Uuid(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref().map_or(serde_json::Value::Null, |u| {
                        serde_json::Value::String(u.to_string())
                    })
                })
                .collect(),
        ),
        etl::types::ArrayCell::Json(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| v.clone().unwrap_or(serde_json::Value::Null))
                .collect(),
        ),
        etl::types::ArrayCell::Bytes(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref().map_or(serde_json::Value::Null, |b| {
                        serde_json::Value::String(encode_base64(b))
                    })
                })
                .collect(),
        ),
    }
}

fn polars_schema_with_metadata(
    schema: &TableSchema,
    metadata: &MetadataColumns,
) -> EtlResult<Schema> {
    let mut fields: Vec<Field> = Vec::with_capacity(schema.columns.len() + 2);
    for column in &schema.columns {
        let dtype = match column.data_type {
            DataType::Int64 => PolarsDataType::Int64,
            DataType::Float64 | DataType::Interval => PolarsDataType::Float64,
            DataType::Bool => PolarsDataType::Boolean,
            _ => PolarsDataType::String,
        };
        fields.push(Field::new(column.name.as_str().into(), dtype));
    }
    fields.push(Field::new(
        metadata.synced_at.as_str().into(),
        PolarsDataType::String,
    ));
    fields.push(Field::new(
        metadata.deleted_at.as_str().into(),
        PolarsDataType::String,
    ));
    Ok(Schema::from_iter(fields))
}

fn encode_base64(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}

fn format_time(value: &NaiveTime) -> String {
    value.format("%H:%M:%S%.f").to_string()
}
