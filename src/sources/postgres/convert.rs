use super::*;

pub(super) fn pg_type_to_data_type(data_type: &str) -> DataType {
    match data_type {
        "smallint" | "integer" | "bigint" => DataType::Int64,
        "real" | "double precision" => DataType::Float64,
        "numeric" | "decimal" => DataType::Numeric,
        "boolean" => DataType::Bool,
        "timestamp without time zone" | "timestamp with time zone" => DataType::Timestamp,
        "date" => DataType::Date,
        "interval" => DataType::Interval,
        "json" | "jsonb" => DataType::Json,
        "bytea" => DataType::Bytes,
        _ => DataType::String,
    }
}

pub(super) fn rows_to_batch(
    schema: &TableSchema,
    rows: &[PgRow],
    table: &ResolvedPostgresTable,
    synced_at: DateTime<Utc>,
    metadata: &MetadataColumns,
) -> Result<DataFrame> {
    let polars_schema = polars_schema_with_metadata(schema, metadata)?;
    let mut output: Vec<PolarsRow> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values: Vec<AnyValue> = Vec::with_capacity(polars_schema.len());
        for column in &schema.columns {
            let value = pg_value_to_anyvalue(row, column)?;
            values.push(value);
        }

        values.push(AnyValue::StringOwned(PlSmallStr::from(
            synced_at.to_rfc3339(),
        )));
        let deleted_at = derive_deleted_at(row, table).unwrap_or(Value::Null);
        match deleted_at {
            Value::String(value) => values.push(AnyValue::StringOwned(PlSmallStr::from(value))),
            _ => values.push(AnyValue::Null),
        }
        output.push(PolarsRow::new(values));
    }

    DataFrame::from_rows_and_schema(&output, &polars_schema).map_err(Into::into)
}

pub(super) fn tokio_rows_to_batch(
    schema: &TableSchema,
    rows: &[TokioPgRow],
    table: &ResolvedPostgresTable,
    synced_at: DateTime<Utc>,
    metadata: &MetadataColumns,
) -> Result<DataFrame> {
    let polars_schema = polars_schema_with_metadata(schema, metadata)?;
    let mut output: Vec<PolarsRow> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values: Vec<AnyValue> = Vec::with_capacity(polars_schema.len());
        for column in &schema.columns {
            let value = tokio_pg_value_to_anyvalue(row, column)?;
            values.push(value);
        }

        values.push(AnyValue::StringOwned(PlSmallStr::from(
            synced_at.to_rfc3339(),
        )));
        let deleted_at = derive_deleted_at_tokio(row, table).unwrap_or(Value::Null);
        match deleted_at {
            Value::String(value) => values.push(AnyValue::StringOwned(PlSmallStr::from(value))),
            _ => values.push(AnyValue::Null),
        }
        output.push(PolarsRow::new(values));
    }

    DataFrame::from_rows_and_schema(&output, &polars_schema).map_err(Into::into)
}

pub(super) fn derive_deleted_at(row: &PgRow, table: &ResolvedPostgresTable) -> Option<Value> {
    if !table.soft_delete {
        return Some(Value::Null);
    }
    let column = table.soft_delete_column.as_ref()?;
    let raw = row.try_get_raw(column.as_str()).ok()?;
    if raw.is_null() {
        return Some(Value::Null);
    }

    if let Ok(flag) = row.try_get::<bool, _>(column.as_str())
        && flag
    {
        return Some(Value::String(Utc::now().to_rfc3339()));
    }

    if let Ok(ts) = row.try_get::<NaiveDateTime, _>(column.as_str()) {
        let dt = Utc.from_utc_datetime(&ts);
        return Some(Value::String(dt.to_rfc3339()));
    }
    if let Ok(ts) = row.try_get::<DateTime<Utc>, _>(column.as_str()) {
        return Some(Value::String(ts.to_rfc3339()));
    }

    if let Ok(date) = row.try_get::<NaiveDate, _>(column.as_str()) {
        return Some(Value::String(date.format("%Y-%m-%d").to_string()));
    }

    Some(Value::Null)
}

pub(super) fn derive_deleted_at_tokio(
    row: &TokioPgRow,
    table: &ResolvedPostgresTable,
) -> Option<Value> {
    if !table.soft_delete {
        return Some(Value::Null);
    }
    let column = table.soft_delete_column.as_ref()?;

    if let Ok(Some(flag)) = row.try_get::<_, Option<bool>>(column.as_str())
        && flag
    {
        return Some(Value::String(Utc::now().to_rfc3339()));
    }

    if let Ok(Some(ts)) = row.try_get::<_, Option<NaiveDateTime>>(column.as_str()) {
        let dt = Utc.from_utc_datetime(&ts);
        return Some(Value::String(dt.to_rfc3339()));
    }
    if let Ok(Some(ts)) = row.try_get::<_, Option<DateTime<Utc>>>(column.as_str()) {
        return Some(Value::String(ts.to_rfc3339()));
    }
    if let Ok(Some(date)) = row.try_get::<_, Option<NaiveDate>>(column.as_str()) {
        return Some(Value::String(date.format("%Y-%m-%d").to_string()));
    }

    Some(Value::Null)
}

pub(super) fn pg_value_to_anyvalue(
    row: &PgRow,
    column: &ColumnSchema,
) -> Result<AnyValue<'static>> {
    let name = column.name.as_str();
    let value = match column.data_type {
        DataType::String => row
            .try_get::<Option<String>, _>(name)?
            .map(|v| AnyValue::StringOwned(PlSmallStr::from(v))),
        DataType::Int64 => {
            if let Ok(value) = row.try_get::<Option<i64>, _>(name) {
                value.map(AnyValue::Int64)
            } else if let Ok(value) = row.try_get::<Option<i32>, _>(name) {
                value.map(|v| AnyValue::Int64(v as i64))
            } else if let Ok(value) = row.try_get::<Option<i16>, _>(name) {
                value.map(|v| AnyValue::Int64(v as i64))
            } else {
                None
            }
        }
        DataType::Float64 => row.try_get::<Option<f64>, _>(name)?.map(AnyValue::Float64),
        DataType::Bool => row.try_get::<Option<bool>, _>(name)?.map(AnyValue::Boolean),
        DataType::Timestamp => {
            if let Ok(value) = row.try_get::<Option<NaiveDateTime>, _>(name) {
                value
                    .map(|v| Utc.from_utc_datetime(&v).to_rfc3339())
                    .map(|v| AnyValue::StringOwned(PlSmallStr::from(v)))
            } else if let Ok(value) = row.try_get::<Option<DateTime<Utc>>, _>(name) {
                value
                    .map(|v| v.to_rfc3339())
                    .map(|v| AnyValue::StringOwned(PlSmallStr::from(v)))
            } else {
                None
            }
        }
        DataType::Date => row
            .try_get::<Option<NaiveDate>, _>(name)?
            .map(|v| v.format("%Y-%m-%d").to_string())
            .map(|v| AnyValue::StringOwned(PlSmallStr::from(v))),
        DataType::Interval => {
            if let Ok(value) = row.try_get::<Option<f64>, _>(name) {
                value.map(AnyValue::Float64)
            } else if let Ok(value) = row.try_get::<Option<String>, _>(name) {
                value
                    .and_then(|v| parse_postgres_interval_to_seconds(&v).ok())
                    .map(AnyValue::Float64)
            } else {
                None
            }
        }
        DataType::Bytes => row
            .try_get::<Option<Vec<u8>>, _>(name)?
            .map(|v| AnyValue::StringOwned(PlSmallStr::from(encode_base64(&v)))),
        DataType::Numeric => {
            if let Ok(value) = row.try_get::<Option<bigdecimal::BigDecimal>, _>(name) {
                value.map(|v| AnyValue::StringOwned(PlSmallStr::from(v.to_string())))
            } else if let Ok(value) = row.try_get::<Option<String>, _>(name) {
                value.map(|v| AnyValue::StringOwned(PlSmallStr::from(v)))
            } else {
                None
            }
        }
        DataType::Json => {
            if let Ok(value) = row.try_get::<Option<serde_json::Value>, _>(name) {
                value.map(|v| AnyValue::StringOwned(PlSmallStr::from(v.to_string())))
            } else if let Ok(value) = row.try_get::<Option<String>, _>(name) {
                value.map(|v| AnyValue::StringOwned(PlSmallStr::from(v)))
            } else {
                None
            }
        }
    };

    Ok(value.unwrap_or(AnyValue::Null))
}

pub(super) fn tokio_pg_value_to_anyvalue(
    row: &TokioPgRow,
    column: &ColumnSchema,
) -> Result<AnyValue<'static>> {
    let name = column.name.as_str();
    let value = match column.data_type {
        DataType::String => row
            .try_get::<_, Option<String>>(name)?
            .map(|v| AnyValue::StringOwned(PlSmallStr::from(v))),
        DataType::Int64 => {
            if let Ok(value) = row.try_get::<_, Option<i64>>(name) {
                value.map(AnyValue::Int64)
            } else if let Ok(value) = row.try_get::<_, Option<i32>>(name) {
                value.map(|v| AnyValue::Int64(v as i64))
            } else if let Ok(value) = row.try_get::<_, Option<i16>>(name) {
                value.map(|v| AnyValue::Int64(v as i64))
            } else {
                None
            }
        }
        DataType::Float64 => {
            if let Ok(value) = row.try_get::<_, Option<f64>>(name) {
                value.map(AnyValue::Float64)
            } else if let Ok(value) = row.try_get::<_, Option<f32>>(name) {
                value.map(|v| AnyValue::Float64(v as f64))
            } else {
                None
            }
        }
        DataType::Bool => row.try_get::<_, Option<bool>>(name)?.map(AnyValue::Boolean),
        DataType::Timestamp => {
            if let Ok(value) = row.try_get::<_, Option<NaiveDateTime>>(name) {
                value
                    .map(|v| Utc.from_utc_datetime(&v).to_rfc3339())
                    .map(|v| AnyValue::StringOwned(PlSmallStr::from(v)))
            } else if let Ok(value) = row.try_get::<_, Option<DateTime<Utc>>>(name) {
                value
                    .map(|v| v.to_rfc3339())
                    .map(|v| AnyValue::StringOwned(PlSmallStr::from(v)))
            } else {
                None
            }
        }
        DataType::Date => row
            .try_get::<_, Option<NaiveDate>>(name)?
            .map(|v| v.format("%Y-%m-%d").to_string())
            .map(|v| AnyValue::StringOwned(PlSmallStr::from(v))),
        DataType::Interval => {
            if let Ok(value) = row.try_get::<_, Option<f64>>(name) {
                value.map(AnyValue::Float64)
            } else if let Ok(value) = row.try_get::<_, Option<String>>(name) {
                value
                    .and_then(|v| parse_postgres_interval_to_seconds(&v).ok())
                    .map(AnyValue::Float64)
            } else {
                None
            }
        }
        DataType::Bytes => row
            .try_get::<_, Option<Vec<u8>>>(name)?
            .map(|v| AnyValue::StringOwned(PlSmallStr::from(encode_base64(&v)))),
        DataType::Numeric => row
            .try_get::<_, Option<String>>(name)?
            .map(|v| AnyValue::StringOwned(PlSmallStr::from(v))),
        DataType::Json => {
            if let Ok(value) = row.try_get::<_, Option<serde_json::Value>>(name) {
                value.map(|v| AnyValue::StringOwned(PlSmallStr::from(v.to_string())))
            } else if let Ok(value) = row.try_get::<_, Option<String>>(name) {
                value.map(|v| AnyValue::StringOwned(PlSmallStr::from(v)))
            } else {
                None
            }
        }
    };

    Ok(value.unwrap_or(AnyValue::Null))
}

pub(super) fn read_updated_at(row: &PgRow, updated_at: &str) -> Option<DateTime<Utc>> {
    if let Ok(ts) = row.try_get::<NaiveDateTime, _>(updated_at) {
        return Some(Utc.from_utc_datetime(&ts));
    }
    if let Ok(ts) = row.try_get::<DateTime<Utc>, _>(updated_at) {
        return Some(ts);
    }
    None
}

pub(super) fn read_primary_key(row: &PgRow, column: &str) -> Result<String> {
    if let Ok(value) = row.try_get::<String, _>(column) {
        return Ok(value);
    }
    if let Ok(value) = row.try_get::<i64, _>(column) {
        return Ok(value.to_string());
    }
    if let Ok(value) = row.try_get::<i32, _>(column) {
        return Ok(value.to_string());
    }
    if let Ok(value) = row.try_get::<BigDecimal, _>(column) {
        return Ok(value.to_string());
    }
    if let Ok(value) = row.try_get::<Uuid, _>(column) {
        return Ok(value.to_string());
    }
    if let Ok(value) = row.try_get::<bool, _>(column) {
        return Ok(value.to_string());
    }
    let raw = row
        .try_get::<serde_json::Value, _>(column)
        .with_context(|| format!("unable to decode primary key column {}", column))?;
    read_primary_key_from_value(raw)
}

pub(super) fn build_incremental_sql(parts: &IncrementalSqlParts<'_>) -> String {
    let mut where_clauses = Vec::new();
    if let Some(where_clause) = parts.where_clause {
        where_clauses.push(format!("({})", where_clause));
    }
    let pagination_clause = if parts.has_last_pk {
        format!(
            "({updated_at} > $1 OR ({updated_at} = $1 AND {pk} > $2::{pk_cast}))",
            updated_at = parts.updated_at,
            pk = parts.primary_key,
            pk_cast = parts.pk_cast
        )
    } else {
        format!("{updated_at} > $1", updated_at = parts.updated_at)
    };
    where_clauses.push(pagination_clause);
    let where_sql = format!(" WHERE {}", where_clauses.join(" AND "));
    let limit_placeholder = if parts.has_last_pk { "$3" } else { "$2" };
    format!(
        "SELECT {columns} FROM {schema}.{table}{where_sql} ORDER BY {updated_at} ASC, {pk} ASC LIMIT {limit}",
        columns = parts.columns,
        schema = parts.schema,
        table = parts.table,
        where_sql = where_sql,
        updated_at = parts.updated_at,
        pk = parts.primary_key,
        limit = limit_placeholder
    )
}

pub(super) fn read_primary_key_from_value(value: Value) -> Result<String> {
    match value {
        Value::String(value) => Ok(value),
        Value::Number(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Null => anyhow::bail!("primary key value is null"),
        other => Ok(other.to_string()),
    }
}

pub(super) fn format_cast_type(udt_name: &str, udt_schema: &str) -> String {
    if udt_schema == "pg_catalog" {
        udt_name.to_string()
    } else {
        format!("{}.{}", udt_schema, udt_name)
    }
}

pub(super) fn encode_base64(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}

pub(super) fn polars_schema_with_metadata(
    schema: &TableSchema,
    metadata: &MetadataColumns,
) -> Result<Schema> {
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

pub(super) fn pg_type_to_data_type_from_type(typ: &etl::types::Type) -> DataType {
    use etl::types::Type;
    match *typ {
        Type::INT2 | Type::INT4 | Type::INT8 => DataType::Int64,
        Type::FLOAT4 | Type::FLOAT8 => DataType::Float64,
        Type::BOOL => DataType::Bool,
        Type::TIMESTAMP | Type::TIMESTAMPTZ => DataType::Timestamp,
        Type::DATE => DataType::Date,
        Type::INTERVAL => DataType::Interval,
        Type::JSON | Type::JSONB => DataType::Json,
        Type::BYTEA => DataType::Bytes,
        Type::NUMERIC => DataType::Numeric,
        _ => DataType::String,
    }
}

pub(super) fn cdc_table_info_from_schema(
    table_cfg: &ResolvedPostgresTable,
    etl_schema: &EtlTableSchema,
    metadata: &MetadataColumns,
) -> Result<CdcTableInfo> {
    let source_columns: Vec<ColumnSchema> = etl_schema
        .column_schemas
        .iter()
        .map(|col| ColumnSchema {
            name: col.name.clone(),
            data_type: pg_type_to_data_type_from_type(&col.typ),
            nullable: col.nullable,
        })
        .collect();

    let required = required_columns(table_cfg);
    ensure_required_columns(&source_columns, &required)?;
    let dest_columns = filter_columns(&source_columns, &table_cfg.columns, &required);

    let schema = TableSchema {
        name: crate::types::destination_table_name(&etl_schema.name.to_string()),
        columns: dest_columns,
        primary_key: Some(table_cfg.primary_key.clone()),
    };

    if !schema
        .columns
        .iter()
        .any(|c| c.name == table_cfg.primary_key)
    {
        anyhow::bail!(
            "primary key {} not found in table {}",
            table_cfg.primary_key,
            etl_schema.name
        );
    }

    CdcTableInfo::new(
        crate::destinations::etl_bigquery::CdcTableSpec {
            table_id: etl_schema.id,
            source_name: etl_schema.name.to_string(),
            dest_name: schema.name.clone(),
            schema,
            metadata: metadata.clone(),
            primary_key: table_cfg.primary_key.clone(),
            soft_delete: table_cfg.soft_delete,
            soft_delete_column: table_cfg.soft_delete_column.clone(),
        },
        etl_schema,
    )
}

pub(super) fn tuple_to_row(
    column_schemas: &[etl_postgres::types::ColumnSchema],
    tuple_data: &[TupleData],
    old_row: Option<&TableRow>,
) -> Result<TableRow> {
    let mut values = Vec::with_capacity(column_schemas.len());

    for (idx, column_schema) in column_schemas.iter().enumerate() {
        let value = match tuple_data.get(idx) {
            Some(TupleData::Null) => Cell::Null,
            Some(TupleData::UnchangedToast) => old_row
                .and_then(|row| row.values.get(idx).cloned())
                .unwrap_or(Cell::Null),
            Some(TupleData::Text(bytes)) => {
                let text = String::from_utf8_lossy(bytes);
                parse_text_cell(&column_schema.typ, &text)
            }
            Some(TupleData::Binary(bytes)) => Cell::Bytes(bytes.to_vec()),
            None => Cell::Null,
        };
        values.push(value);
    }

    Ok(TableRow::new(values))
}

pub(super) fn parse_text_cell(typ: &etl::types::Type, text: &str) -> Cell {
    use etl::types::Type;
    match *typ {
        Type::BOOL => match text {
            "t" | "true" | "TRUE" => Cell::Bool(true),
            "f" | "false" | "FALSE" => Cell::Bool(false),
            _ => Cell::String(text.to_string()),
        },
        Type::INT2 | Type::INT4 | Type::INT8 => text
            .parse::<i64>()
            .map(Cell::I64)
            .unwrap_or_else(|_| Cell::String(text.to_string())),
        Type::FLOAT4 | Type::FLOAT8 => text
            .parse::<f64>()
            .map(Cell::F64)
            .unwrap_or_else(|_| Cell::String(text.to_string())),
        Type::INTERVAL => parse_postgres_interval_to_seconds(text)
            .map(Cell::F64)
            .unwrap_or_else(|_| Cell::String(text.to_string())),
        Type::BYTEA => parse_bytea(text)
            .map(Cell::Bytes)
            .unwrap_or_else(|_| Cell::String(text.to_string())),
        _ => Cell::String(text.to_string()),
    }
}

pub(super) fn parse_postgres_interval_to_seconds(text: &str) -> Result<f64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        anyhow::bail!("empty interval");
    }

    let mut total_seconds = 0.0_f64;
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    let mut idx = 0;

    while idx < tokens.len() {
        let token = tokens[idx];
        if token.contains(':') {
            total_seconds += parse_interval_clock_component(token)?;
            idx += 1;
            continue;
        }

        let value = token
            .parse::<f64>()
            .with_context(|| format!("parsing interval component `{}`", token))?;
        let unit = tokens
            .get(idx + 1)
            .copied()
            .context("interval component missing unit")?;
        total_seconds += interval_unit_seconds(value, unit)?;
        idx += 2;
    }

    Ok(total_seconds)
}

fn parse_interval_clock_component(component: &str) -> Result<f64> {
    let negative = component.starts_with('-');
    let normalized = if component.starts_with(['+', '-']) {
        &component[1..]
    } else {
        component
    };
    let parts: Vec<&str> = normalized.split(':').collect();
    if parts.len() != 3 {
        anyhow::bail!("invalid interval clock component `{}`", component);
    }

    let hours = parts[0]
        .parse::<f64>()
        .with_context(|| format!("parsing interval hours from `{}`", component))?;
    let minutes = parts[1]
        .parse::<f64>()
        .with_context(|| format!("parsing interval minutes from `{}`", component))?;
    let seconds = parts[2]
        .parse::<f64>()
        .with_context(|| format!("parsing interval seconds from `{}`", component))?;
    let total = (hours * 3600.0) + (minutes * 60.0) + seconds;

    Ok(if negative { -total } else { total })
}

fn interval_unit_seconds(value: f64, unit: &str) -> Result<f64> {
    let normalized = unit.to_ascii_lowercase();
    let multiplier = match normalized.as_str() {
        "year" | "years" => 365.25 * 86_400.0,
        "mon" | "mons" | "month" | "months" => 30.0 * 86_400.0,
        "day" | "days" => 86_400.0,
        "hour" | "hours" => 3_600.0,
        "minute" | "minutes" | "min" | "mins" => 60.0,
        "second" | "seconds" | "sec" | "secs" => 1.0,
        _ => anyhow::bail!("unsupported interval unit `{}`", unit),
    };
    Ok(value * multiplier)
}

pub(super) fn parse_bytea(text: &str) -> Result<Vec<u8>> {
    if let Some(hex) = text.strip_prefix("\\x") {
        return hex::decode(hex).context("invalid bytea hex");
    }
    Ok(text.as_bytes().to_vec())
}
