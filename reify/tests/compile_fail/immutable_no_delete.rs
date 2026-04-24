//! Compile-fail test: immutable tables should not have delete() method.

use reify::Table;

#[derive(Table, Debug, Clone)]
#[table(name = "audit_log", immutable)]
pub struct AuditLog {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub event: String,
}

fn main() {
    // This should fail to compile: delete() is not generated for immutable tables
    let _ = AuditLog::delete();
}
