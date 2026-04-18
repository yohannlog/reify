pub mod adapter;
pub mod audit;
pub mod built_query;
pub mod column;
pub mod condition;
pub mod db;
pub mod enumeration;
pub mod func;
pub mod hooks;
pub mod ident;
pub mod migration;
pub mod paginate;
pub mod query;
pub mod range;
pub mod relation;
pub mod rls;
pub mod schema;
pub mod sql;
pub mod table;
pub mod value;
pub mod view;

pub use tracing;

pub use column::Column;
#[cfg(feature = "postgres")]
pub use column::{JsonExpr, JsonPathExpr};
#[cfg(feature = "postgres")]
pub use condition::PgCondition;
pub use condition::{AggregateCondition, Condition, LogicalOp};
#[cfg(feature = "postgres")]
#[allow(deprecated)]
pub use query::rewrite_placeholders_pg;
#[allow(deprecated)]
pub use query::{
    BuildError, DeleteBuilder, Dialect, Expr, InsertBuilder, InsertManyBuilder, JoinClause,
    JoinKind, OnConflict, SelectBuilder, UpdateBuilder, WithBuilder,
};

pub use func::count_all;

pub use relation::{Related, Relation, RelationType};
pub use sql::{JoinFragment, OrderFragment, SqlFragment, ToSql};
pub use table::Table;
pub use value::{FromValue, Value};

pub use schema::{
    ColumnBuilder, ColumnDef, ComputedColumn, ForeignKeyAction, ForeignKeyDef, HasTimestamp,
    IndexBuilder, IndexColumnDef, IndexDef, IndexKind, NoTimestamp, Schema, SortDirection, SqlType,
    TableSchema, TimestampKind, TimestampSource, TimestampState, table,
};

pub use paginate::{
    Cursor, CursorBuilder, CursorCol, CursorDirection, CursorPage, CursorPaginated, Edge, Page,
    PageInfo, Paginated,
};

pub use built_query::BuiltQuery;

pub use db::{
    BoxFuture, Database, DbError, DynDatabase, FromRow, Row, TransactionFn, delete, fetch,
    fetch_all, fetch_one, fetch_optional, insert, insert_many, raw_execute, raw_fetch, raw_query,
    sqlstate, update,
};

#[cfg(feature = "postgres")]
pub use db::{delete_returning, insert_many_returning, insert_returning, update_returning};

pub use migration::{
    ColumnDiff, DbColumnInfo, Migration, MigrationContext, MigrationError, MigrationPlan,
    MigrationRunner, MigrationStatus, SchemaDiff, TableDiff, create_table_sql,
    create_table_sql_with_checks, generate_materialized_view_migration_file,
    generate_migration_file, generate_view_migration_file, normalize_sql_type,
};

pub use rls::{
    Policy, PolicyDecision, RlsContext, RlsError, Scoped, ScopedTransactionFn, scoped_delete,
    scoped_fetch, scoped_fetch_all, scoped_update,
};

pub use audit::{
    ActorId, AuditContext, AuditOperation, Auditable, HMAC_MIN_KEY_BYTES, SecretError,
    audit_column_defs_for, audited_delete, audited_insert, audited_update, values_to_json_string,
    verify_audit_row,
};

pub use enumeration::{DbEnum, enum_from_value};

pub use range::{Bound, Range, RangeElement};

pub use view::{
    View, ViewDef, ViewQuery, ViewSchema, ViewSchemaDef, create_materialized_view_sql,
    create_view_sql, drop_materialized_view_sql, drop_view_sql, refresh_materialized_view_sql,
    view as view_schema,
};
