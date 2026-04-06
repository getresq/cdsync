use super::*;

pub(super) fn pg_type_to_data_type(
    data_type: &str,
    udt_name: &str,
    typtype: &str,
) -> Result<DataType> {
    if matches!(typtype, "c") {
        anyhow::bail!("unsupported PostgreSQL composite type `{}`", udt_name);
    }
    if matches!(typtype, "m") || is_multirange_type_name(udt_name) {
        anyhow::bail!("unsupported PostgreSQL multirange type `{}`", udt_name);
    }
    if matches!(udt_name, "oid8" | "xid8") {
        anyhow::bail!("unsupported PostgreSQL type `{}`", udt_name);
    }
    if data_type == "ARRAY" {
        return Ok(DataType::Json);
    }
    if matches!(typtype, "r") || is_json_like_type_name(udt_name) {
        return Ok(DataType::Json);
    }
    if matches!(typtype, "e") {
        return Ok(DataType::String);
    }

    Ok(match data_type {
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
    })
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

pub(super) fn pg_type_to_data_type_from_type(typ: &etl::types::Type) -> Result<DataType> {
    use etl::types::{Kind, Type};

    if matches!(typ.name(), "oid8" | "xid8") {
        anyhow::bail!("unsupported PostgreSQL type `{}`", typ.name());
    }
    if is_json_like_type_name(typ.name()) {
        return Ok(DataType::Json);
    }

    match typ.kind() {
        Kind::Array(_) => return Ok(DataType::Json),
        Kind::Range(_) => return Ok(DataType::Json),
        Kind::Multirange(_) => {
            anyhow::bail!("unsupported PostgreSQL multirange type `{}`", typ.name())
        }
        Kind::Composite(_) => {
            anyhow::bail!("unsupported PostgreSQL composite type `{}`", typ.name())
        }
        Kind::Domain(inner) => return pg_type_to_data_type_from_type(inner),
        Kind::Enum(_) => return Ok(DataType::String),
        Kind::Simple | Kind::Pseudo => {}
        _ => {}
    }

    Ok(match *typ {
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
    })
}

pub(super) fn cdc_table_info_from_schema(
    table_cfg: &ResolvedPostgresTable,
    etl_schema: &EtlTableSchema,
    metadata: &MetadataColumns,
) -> Result<CdcTableInfo> {
    let source_columns: Vec<ColumnSchema> = etl_schema
        .column_schemas
        .iter()
        .map(|col| {
            Ok(ColumnSchema {
                name: col.name.clone(),
                data_type: pg_type_to_data_type_from_type(&col.typ)?,
                nullable: col.nullable,
            })
        })
        .collect::<Result<_>>()?;

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
                .and_then(|row| row.values().get(idx).map(copy_cell))
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

fn copy_cell(cell: &Cell) -> Cell {
    match cell {
        Cell::Null => Cell::Null,
        Cell::Bool(value) => Cell::Bool(*value),
        Cell::String(value) => Cell::String(value.clone()),
        Cell::I16(value) => Cell::I16(*value),
        Cell::I32(value) => Cell::I32(*value),
        Cell::U32(value) => Cell::U32(*value),
        Cell::I64(value) => Cell::I64(*value),
        Cell::F32(value) => Cell::F32(*value),
        Cell::F64(value) => Cell::F64(*value),
        Cell::Numeric(value) => Cell::Numeric(value.clone()),
        Cell::Date(value) => Cell::Date(*value),
        Cell::Time(value) => Cell::Time(*value),
        Cell::Timestamp(value) => Cell::Timestamp(*value),
        Cell::TimestampTz(value) => Cell::TimestampTz(*value),
        Cell::Uuid(value) => Cell::Uuid(*value),
        Cell::Json(value) => Cell::Json(value.clone()),
        Cell::Bytes(value) => Cell::Bytes(value.clone()),
        Cell::Array(value) => Cell::Array(copy_array_cell(value)),
    }
}

fn copy_array_cell(cell: &etl::types::ArrayCell) -> etl::types::ArrayCell {
    match cell {
        etl::types::ArrayCell::Bool(values) => etl::types::ArrayCell::Bool(values.clone()),
        etl::types::ArrayCell::String(values) => etl::types::ArrayCell::String(values.clone()),
        etl::types::ArrayCell::I16(values) => etl::types::ArrayCell::I16(values.clone()),
        etl::types::ArrayCell::I32(values) => etl::types::ArrayCell::I32(values.clone()),
        etl::types::ArrayCell::U32(values) => etl::types::ArrayCell::U32(values.clone()),
        etl::types::ArrayCell::I64(values) => etl::types::ArrayCell::I64(values.clone()),
        etl::types::ArrayCell::F32(values) => etl::types::ArrayCell::F32(values.clone()),
        etl::types::ArrayCell::F64(values) => etl::types::ArrayCell::F64(values.clone()),
        etl::types::ArrayCell::Numeric(values) => etl::types::ArrayCell::Numeric(values.clone()),
        etl::types::ArrayCell::Date(values) => etl::types::ArrayCell::Date(values.clone()),
        etl::types::ArrayCell::Time(values) => etl::types::ArrayCell::Time(values.clone()),
        etl::types::ArrayCell::Timestamp(values) => {
            etl::types::ArrayCell::Timestamp(values.clone())
        }
        etl::types::ArrayCell::TimestampTz(values) => {
            etl::types::ArrayCell::TimestampTz(values.clone())
        }
        etl::types::ArrayCell::Uuid(values) => etl::types::ArrayCell::Uuid(values.clone()),
        etl::types::ArrayCell::Json(values) => etl::types::ArrayCell::Json(values.clone()),
        etl::types::ArrayCell::Bytes(values) => etl::types::ArrayCell::Bytes(values.clone()),
    }
}

pub(super) fn parse_text_cell(typ: &etl::types::Type, text: &str) -> Cell {
    use etl::types::{Kind, Type};

    if is_json_like_type_name(typ.name()) {
        return Cell::Json(Value::String(text.to_string()));
    }

    match typ.kind() {
        Kind::Array(inner) => {
            return parse_text_array_cell(inner, text)
                .unwrap_or_else(|_| Cell::String(text.to_string()));
        }
        Kind::Range(_) => {
            return Cell::Json(Value::String(text.to_string()));
        }
        Kind::Domain(inner) => return parse_text_cell(inner, text),
        Kind::Enum(_) => return Cell::String(text.to_string()),
        Kind::Multirange(_) | Kind::Composite(_) => return Cell::String(text.to_string()),
        Kind::Simple | Kind::Pseudo => {}
        _ => {}
    }

    match *typ {
        Type::BOOL => match text {
            "t" | "true" | "TRUE" => Cell::Bool(true),
            "f" | "false" | "FALSE" => Cell::Bool(false),
            _ => Cell::String(text.to_string()),
        },
        Type::INT2 | Type::INT4 | Type::INT8 => text
            .parse::<i64>()
            .map_or_else(|_| Cell::String(text.to_string()), Cell::I64),
        Type::FLOAT4 | Type::FLOAT8 => text
            .parse::<f64>()
            .map_or_else(|_| Cell::String(text.to_string()), Cell::F64),
        Type::INTERVAL => parse_postgres_interval_to_seconds(text)
            .map_or_else(|_| Cell::String(text.to_string()), Cell::F64),
        Type::BYTEA => parse_bytea(text)
            .map_or_else(|_| Cell::String(text.to_string()), Cell::Bytes),
        _ => Cell::String(text.to_string()),
    }
}

fn parse_text_array_cell(inner: &etl::types::Type, text: &str) -> Result<Cell> {
    use etl::types::{ArrayCell, Kind, Type};

    match inner.kind() {
        Kind::Domain(base) => return parse_text_array_cell(base, text),
        Kind::Enum(_) => {
            return parse_postgres_text_array(
                text,
                |value| Ok(Some(value.to_string())),
                ArrayCell::String,
            );
        }
        Kind::Array(_) => {
            return parse_postgres_text_array(
                text,
                |value| Ok(Some(value.to_string())),
                ArrayCell::String,
            );
        }
        Kind::Range(_) => {
            return parse_postgres_text_array(
                text,
                |value| Ok(Some(Value::String(value.to_string()))),
                ArrayCell::Json,
            );
        }
        Kind::Multirange(_) | Kind::Composite(_) => {
            anyhow::bail!(
                "unsupported PostgreSQL array element type `{}`",
                inner.name()
            )
        }
        Kind::Simple | Kind::Pseudo => {}
        _ => {}
    }

    match *inner {
        Type::BOOL => parse_postgres_text_array(
            text,
            |value| match value {
                "t" | "true" | "TRUE" => Ok(Some(true)),
                "f" | "false" | "FALSE" => Ok(Some(false)),
                _ => anyhow::bail!("invalid boolean array value `{}`", value),
            },
            ArrayCell::Bool,
        ),
        Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
            parse_postgres_text_array(text, |value| Ok(Some(value.to_string())), ArrayCell::String)
        }
        Type::INT2 => parse_postgres_text_array(
            text,
            |value| Ok(Some(value.parse::<i16>()?)),
            ArrayCell::I16,
        ),
        Type::INT4 => parse_postgres_text_array(
            text,
            |value| Ok(Some(value.parse::<i32>()?)),
            ArrayCell::I32,
        ),
        Type::INT8 => parse_postgres_text_array(
            text,
            |value| Ok(Some(value.parse::<i64>()?)),
            ArrayCell::I64,
        ),
        Type::FLOAT4 => parse_postgres_text_array(
            text,
            |value| Ok(Some(value.parse::<f32>()?)),
            ArrayCell::F32,
        ),
        Type::FLOAT8 | Type::INTERVAL => parse_postgres_text_array(
            text,
            |value| {
                if matches!(*inner, Type::INTERVAL) {
                    Ok(Some(parse_postgres_interval_to_seconds(value)?))
                } else {
                    Ok(Some(value.parse::<f64>()?))
                }
            },
            ArrayCell::F64,
        ),
        Type::NUMERIC => {
            parse_postgres_text_array(text, |value| Ok(Some(value.parse()?)), ArrayCell::Numeric)
        }
        Type::DATE => parse_postgres_text_array(
            text,
            |value| Ok(Some(NaiveDate::parse_from_str(value, "%Y-%m-%d")?)),
            ArrayCell::Date,
        ),
        Type::TIME => parse_postgres_text_array(
            text,
            |value| Ok(Some(NaiveTime::parse_from_str(value, "%H:%M:%S%.f")?)),
            ArrayCell::Time,
        ),
        Type::TIMESTAMP => parse_postgres_text_array(
            text,
            |value| {
                Ok(Some(NaiveDateTime::parse_from_str(
                    value,
                    "%Y-%m-%d %H:%M:%S%.f",
                )?))
            },
            ArrayCell::Timestamp,
        ),
        Type::TIMESTAMPTZ => parse_postgres_text_array(
            text,
            |value| Ok(Some(parse_timestamptz_text(value)?)),
            ArrayCell::TimestampTz,
        ),
        Type::UUID => parse_postgres_text_array(
            text,
            |value| Ok(Some(Uuid::parse_str(value)?)),
            ArrayCell::Uuid,
        ),
        Type::JSON | Type::JSONB => parse_postgres_text_array(
            text,
            |value| Ok(Some(serde_json::from_str(value)?)),
            ArrayCell::Json,
        ),
        Type::OID => parse_postgres_text_array(
            text,
            |value| Ok(Some(value.parse::<u32>()?)),
            ArrayCell::U32,
        ),
        Type::BYTEA => parse_postgres_text_array(
            text,
            |value| Ok(Some(parse_bytea(value)?)),
            ArrayCell::Bytes,
        ),
        _ => {
            parse_postgres_text_array(text, |value| Ok(Some(value.to_string())), ArrayCell::String)
        }
    }
}

fn parse_postgres_text_array<P, M, T>(text: &str, mut parse: P, map: M) -> Result<Cell>
where
    P: FnMut(&str) -> Result<Option<T>>,
    M: FnOnce(Vec<Option<T>>) -> etl::types::ArrayCell,
{
    if text.len() < 2 {
        anyhow::bail!("array input too short");
    }
    if !text.starts_with('{') || !text.ends_with('}') {
        anyhow::bail!("array input missing braces");
    }

    let mut values = Vec::new();
    let text = &text[1..(text.len() - 1)];
    let mut value_text = String::with_capacity(10);
    let mut in_quotes = false;
    let mut in_escape = false;
    let mut value_quoted = false;
    let mut chars = text.chars();
    let mut done = text.is_empty();

    while !done {
        loop {
            match chars.next() {
                Some(ch) => match ch {
                    ch if in_escape => {
                        value_text.push(ch);
                        in_escape = false;
                    }
                    '"' => {
                        if !in_quotes {
                            value_quoted = true;
                        }
                        in_quotes = !in_quotes;
                    }
                    '\\' => in_escape = true,
                    ',' if !in_quotes => break,
                    ch => value_text.push(ch),
                },
                None => {
                    done = true;
                    break;
                }
            }
        }

        let value = if !value_quoted && value_text.eq_ignore_ascii_case("null") {
            None
        } else {
            parse(&value_text)?
        };
        values.push(value);
        value_text.clear();
        value_quoted = false;
    }

    Ok(Cell::Array(map(values)))
}

fn parse_timestamptz_text(value: &str) -> Result<DateTime<Utc>> {
    let parsed = match DateTime::<FixedOffset>::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%#z") {
        Ok(value) => value,
        Err(_) => DateTime::<FixedOffset>::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%:z")?,
    };
    Ok(parsed.into())
}

fn is_json_like_type_name(name: &str) -> bool {
    matches!(
        name,
        "hstore"
            | "geometry"
            | "geography"
            | "box"
            | "circle"
            | "line"
            | "lseg"
            | "path"
            | "point"
            | "polygon"
            | "daterange"
            | "int4range"
            | "int8range"
            | "numrange"
            | "tsrange"
            | "tstzrange"
    )
}

fn is_multirange_type_name(name: &str) -> bool {
    matches!(
        name,
        "datemultirange"
            | "int4multirange"
            | "int8multirange"
            | "nummultirange"
            | "tsmultirange"
            | "tstzmultirange"
    )
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
