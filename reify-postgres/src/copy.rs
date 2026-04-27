use reify_core::db::DbError;
use reify_core::schema::{SqlType, TimestampSource};
use reify_core::table::Table;
use reify_core::value::Value;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;

use crate::{PgValue, PostgresDb, get_conn, pg_err};

impl PostgresDb {
    /// Bulk-insert rows using PostgreSQL's binary COPY protocol.
    ///
    /// This is significantly faster than individual INSERT statements
    /// for large batches.
    ///
    /// Only writable columns (excluding computed and DB-managed timestamps)
    /// are included.
    pub async fn copy_in<T: Table>(&self, models: &[T]) -> Result<u64, DbError> {
        let cols = T::writable_column_names();
        if cols.is_empty() || models.is_empty() {
            return Ok(0);
        }

        let sql = format!(
            "COPY {} ({}) FROM STDIN BINARY",
            T::table_name(),
            cols.join(", ")
        );

        let conn = get_conn(&self.pool, self.acquire_timeout).await?;
        let sink = conn.copy_in(&sql).await.map_err(pg_err)?;

        let defs = T::column_defs();
        let types: Vec<Type> = if !defs.is_empty() {
            defs.iter()
                .filter(|d| d.computed.is_none() && d.timestamp_source != TimestampSource::Db)
                .map(|d| sql_type_to_pg(&d.sql_type))
                .collect()
        } else {
            let first = &models[0];
            first
                .writable_values()
                .iter()
                .map(value_to_pg_type)
                .collect()
        };

        let mut writer = std::pin::pin!(BinaryCopyInWriter::new(sink, &types));

        for model in models {
            let values = model.writable_values();
            let pg_values: Vec<PgValue> = values.iter().map(PgValue).collect();
            let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = pg_values
                .iter()
                .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            writer.as_mut().write(&refs).await.map_err(pg_err)?;
        }

        writer.finish().await.map_err(pg_err)
    }
}

fn sql_type_to_pg(ty: &SqlType) -> Type {
    match ty {
        SqlType::SmallInt => Type::INT2,
        SqlType::Integer => Type::INT4,
        SqlType::BigInt => Type::INT8,
        SqlType::Float => Type::FLOAT4,
        SqlType::Double => Type::FLOAT8,
        SqlType::Boolean => Type::BOOL,
        SqlType::Text | SqlType::Varchar(_) | SqlType::Char(_) | SqlType::Binary(_) => Type::TEXT,
        SqlType::Bytea => Type::BYTEA,
        SqlType::Uuid => Type::UUID,
        SqlType::Timestamptz => Type::TIMESTAMPTZ,
        SqlType::Timestamp => Type::TIMESTAMP,
        SqlType::Date => Type::DATE,
        SqlType::Time => Type::TIME,
        SqlType::Jsonb => Type::JSONB,
        SqlType::Numeric => Type::NUMERIC,
        SqlType::BigSerial | SqlType::Serial => Type::INT8,
        SqlType::Custom(_) => Type::TEXT,
        SqlType::Array(inner) => {
            let element = sql_type_to_pg(inner);
            match element {
                Type::INT2 => Type::INT2_ARRAY,
                Type::INT4 => Type::INT4_ARRAY,
                Type::INT8 => Type::INT8_ARRAY,
                Type::FLOAT4 => Type::FLOAT4_ARRAY,
                Type::FLOAT8 => Type::FLOAT8_ARRAY,
                Type::BOOL => Type::BOOL_ARRAY,
                Type::TEXT | Type::VARCHAR | Type::BPCHAR => Type::TEXT_ARRAY,
                Type::UUID => Type::UUID_ARRAY,
                _ => Type::TEXT_ARRAY,
            }
        }
        SqlType::Decimal(_, _) => Type::NUMERIC,
        SqlType::Vector(_) => Type::TEXT,
        SqlType::Point => Type::POINT,
        SqlType::Inet => Type::INET,
        SqlType::Cidr => Type::CIDR,
        SqlType::MacAddr => Type::MACADDR,
        SqlType::Interval => Type::INTERVAL,
    }
}

fn value_to_pg_type(value: &Value) -> Type {
    match value {
        Value::Null => Type::TEXT,
        Value::Bool(_) => Type::BOOL,
        Value::I16(_) => Type::INT2,
        Value::I32(_) => Type::INT4,
        Value::I64(_) => Type::INT8,
        // No native u64 in PostgreSQL — bind as INT8 (the to_sql conversion
        // refuses values above i64::MAX so we won't truncate silently).
        Value::U64(_) => Type::INT8,
        Value::F32(_) => Type::FLOAT4,
        Value::F64(_) => Type::FLOAT8,
        Value::String(_) => Type::TEXT,
        Value::Bytes(_) => Type::BYTEA,
        Value::Uuid(_) => Type::UUID,
        Value::Timestamptz(_) => Type::TIMESTAMPTZ,
        Value::Timestamp(_) => Type::TIMESTAMP,
        Value::Date(_) => Type::DATE,
        Value::Time(_) => Type::TIME,
        Value::Duration(_) => Type::INTERVAL,
        Value::Jsonb(_) => Type::JSONB,
        Value::Int4Range(_) => Type::INT4_RANGE,
        Value::Int8Range(_) => Type::INT8_RANGE,
        Value::TsRange(_) => Type::TS_RANGE,
        Value::TstzRange(_) => Type::TSTZ_RANGE,
        Value::DateRange(_) => Type::DATE_RANGE,
        Value::ArrayBool(_) => Type::BOOL_ARRAY,
        Value::ArrayI16(_) => Type::INT2_ARRAY,
        Value::ArrayI32(_) => Type::INT4_ARRAY,
        Value::ArrayI64(_) => Type::INT8_ARRAY,
        Value::ArrayF32(_) => Type::FLOAT4_ARRAY,
        Value::ArrayF64(_) => Type::FLOAT8_ARRAY,
        Value::ArrayString(_) => Type::TEXT_ARRAY,
        Value::ArrayUuid(_) => Type::UUID_ARRAY,
        Value::Point(_) => Type::POINT,
        Value::Inet(_) => Type::INET,
        Value::Cidr(_) => Type::CIDR,
        Value::MacAddr(_) => Type::MACADDR,
        Value::Interval(_) => Type::INTERVAL,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_type_to_pg_primitives() {
        assert_eq!(sql_type_to_pg(&SqlType::SmallInt), Type::INT2);
        assert_eq!(sql_type_to_pg(&SqlType::Integer), Type::INT4);
        assert_eq!(sql_type_to_pg(&SqlType::BigInt), Type::INT8);
        assert_eq!(sql_type_to_pg(&SqlType::Float), Type::FLOAT4);
        assert_eq!(sql_type_to_pg(&SqlType::Double), Type::FLOAT8);
        assert_eq!(sql_type_to_pg(&SqlType::Boolean), Type::BOOL);
        assert_eq!(sql_type_to_pg(&SqlType::Text), Type::TEXT);
        assert_eq!(sql_type_to_pg(&SqlType::Varchar(255)), Type::TEXT);
        assert_eq!(sql_type_to_pg(&SqlType::Char(1)), Type::TEXT);
        assert_eq!(sql_type_to_pg(&SqlType::Bytea), Type::BYTEA);
        assert_eq!(sql_type_to_pg(&SqlType::Uuid), Type::UUID);
        assert_eq!(sql_type_to_pg(&SqlType::Timestamptz), Type::TIMESTAMPTZ);
        assert_eq!(sql_type_to_pg(&SqlType::Timestamp), Type::TIMESTAMP);
        assert_eq!(sql_type_to_pg(&SqlType::Date), Type::DATE);
        assert_eq!(sql_type_to_pg(&SqlType::Time), Type::TIME);
        assert_eq!(sql_type_to_pg(&SqlType::Jsonb), Type::JSONB);
        assert_eq!(sql_type_to_pg(&SqlType::Numeric), Type::NUMERIC);
    }

    #[test]
    fn sql_type_to_pg_serials() {
        assert_eq!(sql_type_to_pg(&SqlType::Serial), Type::INT8);
        assert_eq!(sql_type_to_pg(&SqlType::BigSerial), Type::INT8);
    }

    #[test]
    fn sql_type_to_pg_custom_fallback() {
        assert_eq!(sql_type_to_pg(&SqlType::Custom("INET")), Type::TEXT);
    }

    #[test]
    fn sql_type_to_pg_decimal() {
        assert_eq!(sql_type_to_pg(&SqlType::Decimal(10, 2)), Type::NUMERIC);
    }

    #[test]
    fn sql_type_to_pg_arrays() {
        assert_eq!(
            sql_type_to_pg(&SqlType::Array(Box::new(SqlType::Integer))),
            Type::INT4_ARRAY
        );
        assert_eq!(
            sql_type_to_pg(&SqlType::Array(Box::new(SqlType::Text))),
            Type::TEXT_ARRAY
        );
        assert_eq!(
            sql_type_to_pg(&SqlType::Array(Box::new(SqlType::Boolean))),
            Type::BOOL_ARRAY
        );
        assert_eq!(
            sql_type_to_pg(&SqlType::Array(Box::new(SqlType::Uuid))),
            Type::UUID_ARRAY
        );
        assert_eq!(
            sql_type_to_pg(&SqlType::Array(Box::new(SqlType::BigInt))),
            Type::INT8_ARRAY
        );
        assert_eq!(
            sql_type_to_pg(&SqlType::Array(Box::new(SqlType::Float))),
            Type::FLOAT4_ARRAY
        );
        assert_eq!(
            sql_type_to_pg(&SqlType::Array(Box::new(SqlType::Double))),
            Type::FLOAT8_ARRAY
        );
        assert_eq!(
            sql_type_to_pg(&SqlType::Array(Box::new(SqlType::Varchar(100)))),
            Type::TEXT_ARRAY
        );
    }

    #[test]
    fn sql_type_to_pg_nested_array_fallback() {
        assert_eq!(
            sql_type_to_pg(&SqlType::Array(Box::new(SqlType::Array(Box::new(
                SqlType::Integer
            ))))),
            Type::TEXT_ARRAY
        );
    }

    #[test]
    fn value_to_pg_type_primitives() {
        assert_eq!(value_to_pg_type(&Value::Null), Type::TEXT);
        assert_eq!(value_to_pg_type(&Value::Bool(true)), Type::BOOL);
        assert_eq!(value_to_pg_type(&Value::I16(1)), Type::INT2);
        assert_eq!(value_to_pg_type(&Value::I32(1)), Type::INT4);
        assert_eq!(value_to_pg_type(&Value::I64(1)), Type::INT8);
        assert_eq!(value_to_pg_type(&Value::F32(1.0)), Type::FLOAT4);
        assert_eq!(value_to_pg_type(&Value::F64(1.0)), Type::FLOAT8);
        assert_eq!(value_to_pg_type(&Value::String("a".into())), Type::TEXT);
        assert_eq!(value_to_pg_type(&Value::Bytes(vec![0x01])), Type::BYTEA);
    }

    #[test]
    fn value_to_pg_type_temporal_and_json() {
        use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
        use uuid::Uuid;

        assert_eq!(
            value_to_pg_type(&Value::Timestamptz(DateTime::<Utc>::UNIX_EPOCH)),
            Type::TIMESTAMPTZ
        );
        assert_eq!(
            value_to_pg_type(&Value::Timestamp(NaiveDateTime::MIN)),
            Type::TIMESTAMP
        );
        assert_eq!(value_to_pg_type(&Value::Date(NaiveDate::MIN)), Type::DATE);
        assert_eq!(value_to_pg_type(&Value::Time(NaiveTime::MIN)), Type::TIME);
        assert_eq!(
            value_to_pg_type(&Value::Jsonb(serde_json::json!({}))),
            Type::JSONB
        );
        assert_eq!(value_to_pg_type(&Value::Uuid(Uuid::nil())), Type::UUID);
    }

    #[test]
    fn value_to_pg_type_ranges() {
        use reify_core::range::Range;
        assert_eq!(
            value_to_pg_type(&Value::Int4Range(Range::Empty)),
            Type::INT4_RANGE
        );
        assert_eq!(
            value_to_pg_type(&Value::Int8Range(Range::Empty)),
            Type::INT8_RANGE
        );
        assert_eq!(
            value_to_pg_type(&Value::TsRange(Range::Empty)),
            Type::TS_RANGE
        );
        assert_eq!(
            value_to_pg_type(&Value::TstzRange(Range::Empty)),
            Type::TSTZ_RANGE
        );
        assert_eq!(
            value_to_pg_type(&Value::DateRange(Range::Empty)),
            Type::DATE_RANGE
        );
    }

    #[test]
    fn value_to_pg_type_arrays() {
        assert_eq!(
            value_to_pg_type(&Value::ArrayBool(vec![])),
            Type::BOOL_ARRAY
        );
        assert_eq!(value_to_pg_type(&Value::ArrayI16(vec![])), Type::INT2_ARRAY);
        assert_eq!(value_to_pg_type(&Value::ArrayI32(vec![])), Type::INT4_ARRAY);
        assert_eq!(value_to_pg_type(&Value::ArrayI64(vec![])), Type::INT8_ARRAY);
        assert_eq!(
            value_to_pg_type(&Value::ArrayF32(vec![])),
            Type::FLOAT4_ARRAY
        );
        assert_eq!(
            value_to_pg_type(&Value::ArrayF64(vec![])),
            Type::FLOAT8_ARRAY
        );
        assert_eq!(
            value_to_pg_type(&Value::ArrayString(vec![])),
            Type::TEXT_ARRAY
        );
        assert_eq!(
            value_to_pg_type(&Value::ArrayUuid(vec![])),
            Type::UUID_ARRAY
        );
    }
}
