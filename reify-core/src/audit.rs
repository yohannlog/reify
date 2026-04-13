use crate::db::{Database, DbError, FromRow};
use crate::query::{DeleteBuilder, UpdateBuilder};
use crate::schema::{ColumnDef, SqlType, TimestampSource};
use crate::table::Table;

// ── AuditOperation ───────────────────────────────────────────────────

/// Operation kind recorded in the audit log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditOperation {
    Update,
    Delete,
}

impl AuditOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            AuditOperation::Update => "update",
            AuditOperation::Delete => "delete",
        }
    }
}

// ── AuditContext ─────────────────────────────────────────────────────

/// Context passed to audited operations (who triggered the change).
pub struct AuditContext {
    pub actor_id: Option<i64>,
}

// ── Auditable trait ──────────────────────────────────────────────────

/// Implemented automatically by `#[table(audit)]`.
pub trait Auditable: Table {
    /// Name of the audit table (e.g. `"users_audit"`).
    fn audit_table_name() -> &'static str;
    /// Fixed column defs for the audit table.
    fn audit_column_defs() -> Vec<ColumnDef>;
}

// ── Fixed audit column defs ──────────────────────────────────────────

/// Returns the 5 fixed column definitions for any audit table.
pub fn audit_column_defs_for(table_name: &str) -> Vec<ColumnDef> {
    let _ = table_name; // reserved for future per-table customisation
    vec![
        ColumnDef {
            name: "audit_id",
            sql_type: SqlType::BigSerial,
            primary_key: true,
            auto_increment: true,
            unique: false,
            index: false,
            nullable: false,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: TimestampSource::Vm,
            check: None,
        },
        ColumnDef {
            name: "operation",
            sql_type: SqlType::Text,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: false,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: TimestampSource::Vm,
            check: None,
        },
        ColumnDef {
            name: "actor_id",
            sql_type: SqlType::BigInt,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: true,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: TimestampSource::Vm,
            check: None,
        },
        ColumnDef {
            name: "changed_at",
            sql_type: SqlType::Timestamptz,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: false,
            default: Some("NOW()".to_string()),
            computed: None,
            timestamp_kind: None,
            timestamp_source: TimestampSource::Db,
            check: None,
        },
        ColumnDef {
            name: "row_data",
            sql_type: SqlType::Jsonb,
            primary_key: false,
            auto_increment: false,
            unique: false,
            index: false,
            nullable: false,
            default: None,
            computed: None,
            timestamp_kind: None,
            timestamp_source: TimestampSource::Vm,
            check: None,
        },
    ]
}

// ── JSON serialisation helper ────────────────────────────────────────

/// Serialise column-value pairs to a JSON object string without any external crate.
///
/// ```
/// use reify_core::audit::values_to_json_string;
/// use reify_core::Value;
/// let json = values_to_json_string(&["id", "name"], &[Value::Int(1), Value::String("alice".into())]);
/// assert_eq!(json, r#"{"id":1,"name":"alice"}"#);
/// ```
pub fn values_to_json_string(cols: &[&str], vals: &[crate::value::Value]) -> String {
    use crate::value::Value;

    let mut out = String::from("{");
    for (i, (col, val)) in cols.iter().zip(vals.iter()).enumerate() {
        if i > 0 {
            out.push(',');
        }
        // key
        out.push('"');
        out.push_str(col);
        out.push_str("\":");
        // value
        match val {
            Value::Null => out.push_str("null"),
            Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Value::I16(n) => out.push_str(&n.to_string()),
            Value::I32(n) => out.push_str(&n.to_string()),
            Value::I64(n) => out.push_str(&n.to_string()),
            Value::F32(f) => out.push_str(&f.to_string()),
            Value::F64(f) => out.push_str(&f.to_string()),
            Value::String(s) => {
                out.push('"');
                out.push_str(&s.replace('\\', "\\\\").replace('"', "\\\""));
                out.push('"');
            }
            Value::Bytes(b) => {
                // hex encoding for binary data
                out.push('"');
                for byte in b {
                    out.push_str(&format!("{byte:02x}"));
                }
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::Uuid(u) => {
                out.push('"');
                out.push_str(&u.to_string());
                out.push('"');
            }
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Timestamp(ts) => {
                out.push('"');
                out.push_str(&ts.to_string());
                out.push('"');
            }
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Date(d) => {
                out.push('"');
                out.push_str(&d.to_string());
                out.push('"');
            }
            #[cfg(any(feature = "postgres", feature = "mysql"))]
            Value::Time(t) => {
                out.push('"');
                out.push_str(&t.to_string());
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::Timestamptz(ts) => {
                out.push('"');
                out.push_str(&ts.to_string());
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::Jsonb(j) => out.push_str(&j.to_string()),
            // Range and array types: serialize as quoted string representation
            #[cfg(feature = "postgres")]
            Value::Int4Range(r) => {
                out.push('"');
                out.push_str(&format!("{r:?}"));
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::Int8Range(r) => {
                out.push('"');
                out.push_str(&format!("{r:?}"));
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::TsRange(r) => {
                out.push('"');
                out.push_str(&format!("{r:?}"));
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::TstzRange(r) => {
                out.push('"');
                out.push_str(&format!("{r:?}"));
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::DateRange(r) => {
                out.push('"');
                out.push_str(&format!("{r:?}"));
                out.push('"');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayBool(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 { out.push(','); }
                    out.push_str(if *v { "true" } else { "false" });
                }
                out.push(']');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayI16(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 { out.push(','); }
                    out.push_str(&v.to_string());
                }
                out.push(']');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayI32(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 { out.push(','); }
                    out.push_str(&v.to_string());
                }
                out.push(']');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayI64(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 { out.push(','); }
                    out.push_str(&v.to_string());
                }
                out.push(']');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayF32(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 { out.push(','); }
                    out.push_str(&v.to_string());
                }
                out.push(']');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayF64(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 { out.push(','); }
                    out.push_str(&v.to_string());
                }
                out.push(']');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayString(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 { out.push(','); }
                    out.push('"');
                    out.push_str(&v.replace('\\', "\\\\").replace('"', "\\\""));
                    out.push('"');
                }
                out.push(']');
            }
            #[cfg(feature = "postgres")]
            Value::ArrayUuid(arr) => {
                out.push('[');
                for (j, v) in arr.iter().enumerate() {
                    if j > 0 { out.push(','); }
                    out.push('"');
                    out.push_str(&v.to_string());
                    out.push('"');
                }
                out.push(']');
            }
        }
    }
    out.push('}');
    out
}

// ── audited_update ───────────────────────────────────────────────────

/// Execute an UPDATE and write an audit row atomically inside a transaction.
///
/// The audit row captures the operation kind (`"update"`) and the actor id.
/// Because the old values are not fetched (UPDATE does not return old data
/// without a RETURNING clause), `row_data` is set to `"{}"` for UPDATE — use
/// `audited_delete` when you need the full old snapshot.
pub async fn audited_update<M: Auditable>(
    db: &impl Database,
    builder: UpdateBuilder<M>,
    ctx: &AuditContext,
) -> Result<u64, DbError> {
    let (update_sql, update_params) = builder.build();
    let audit_table = M::audit_table_name();
    let actor_id = ctx.actor_id;

    // Build audit INSERT SQL
    let audit_sql = format!(
        "INSERT INTO {audit_table} (operation, actor_id, row_data) VALUES (?, ?, ?)"
    );
    let actor_val = match actor_id {
        Some(id) => crate::value::Value::I64(id),
        None => crate::value::Value::Null,
    };
    let audit_params = vec![
        crate::value::Value::String("update".into()),
        actor_val,
        crate::value::Value::String("{}".into()),
    ];

    let affected = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let affected_clone = affected.clone();

    db.transaction(Box::new(move |tx| {
        Box::pin(async move {
            let n = tx.execute(&update_sql, &update_params).await?;
            affected_clone.store(n, std::sync::atomic::Ordering::Relaxed);
            tx.execute(&audit_sql, &audit_params).await?;
            Ok(())
        })
    })).await?;

    Ok(affected.load(std::sync::atomic::Ordering::Relaxed))
}

// ── audited_delete ───────────────────────────────────────────────────

/// SELECT matching rows, DELETE them, and write one audit row per deleted record —
/// all inside a single transaction.
pub async fn audited_delete<M: Auditable + FromRow>(
    db: &impl Database,
    builder: DeleteBuilder<M>,
    ctx: &AuditContext,
) -> Result<u64, DbError> {
    // 1. Capture old rows before deletion (outside the transaction — read-only).
    let select = builder.to_select();
    let (select_sql, select_params) = select.build();
    let old_rows = db.query(&select_sql, &select_params).await?;

    // Serialize each row to JSON now (before moving into the closure).
    let col_names: Vec<&'static str> = M::column_names().to_vec();
    let mut row_data_list: Vec<String> = Vec::with_capacity(old_rows.len());
    for row in &old_rows {
        let vals: Vec<crate::value::Value> = col_names
            .iter()
            .map(|c| row.get(c).cloned().unwrap_or(crate::value::Value::Null))
            .collect();
        let col_refs: Vec<&str> = col_names.iter().map(|s| *s).collect();
        row_data_list.push(values_to_json_string(&col_refs, &vals));
    }

    let (delete_sql, delete_params) = builder.build();
    let audit_table = M::audit_table_name();
    let actor_id = ctx.actor_id;

    let audit_sql = format!(
        "INSERT INTO {audit_table} (operation, actor_id, row_data) VALUES (?, ?, ?)"
    );

    let affected = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let affected_clone = affected.clone();

    db.transaction(Box::new(move |tx| {
        Box::pin(async move {
            let n = tx.execute(&delete_sql, &delete_params).await?;
            affected_clone.store(n, std::sync::atomic::Ordering::Relaxed);
            for row_data in &row_data_list {
                let actor_val = match actor_id {
                    Some(id) => crate::value::Value::I64(id),
                    None => crate::value::Value::Null,
                };
                tx.execute(
                    &audit_sql,
                    &[
                        crate::value::Value::String("delete".into()),
                        actor_val,
                        crate::value::Value::String(row_data.clone()),
                    ],
                ).await?;
            }
            Ok(())
        })
    })).await?;

    Ok(affected.load(std::sync::atomic::Ordering::Relaxed))
}

// ── Unit tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn test_values_to_json_string_basic() {
        let json = values_to_json_string(
            &["id", "name", "active"],
            &[Value::I64(42), Value::String("alice".into()), Value::Bool(true)],
        );
        assert_eq!(json, r#"{"id":42,"name":"alice","active":true}"#);
    }

    #[test]
    fn test_values_to_json_string_null() {
        let json = values_to_json_string(&["x"], &[Value::Null]);
        assert_eq!(json, r#"{"x":null}"#);
    }

    #[test]
    fn test_values_to_json_string_empty() {
        let json = values_to_json_string(&[], &[]);
        assert_eq!(json, "{}");
    }

    #[test]
    fn test_values_to_json_string_escaping() {
        let json = values_to_json_string(
            &["msg"],
            &[Value::String(r#"say "hi""#.into())],
        );
        assert_eq!(json, r#"{"msg":"say \"hi\""}"#);
    }

    #[test]
    fn test_audit_column_defs_count() {
        let defs = audit_column_defs_for("users");
        assert_eq!(defs.len(), 5);
        assert_eq!(defs[0].name, "audit_id");
        assert_eq!(defs[1].name, "operation");
        assert_eq!(defs[2].name, "actor_id");
        assert_eq!(defs[3].name, "changed_at");
        assert_eq!(defs[4].name, "row_data");
    }

    #[test]
    fn test_audit_column_defs_types() {
        let defs = audit_column_defs_for("users");
        assert_eq!(defs[0].sql_type, SqlType::BigSerial);
        assert!(defs[0].primary_key);
        assert_eq!(defs[1].sql_type, SqlType::Text);
        assert_eq!(defs[2].sql_type, SqlType::BigInt);
        assert!(defs[2].nullable);
        assert_eq!(defs[3].sql_type, SqlType::Timestamptz);
        assert_eq!(defs[3].default, Some("NOW()".to_string()));
        assert_eq!(defs[4].sql_type, SqlType::Jsonb);
    }
}
