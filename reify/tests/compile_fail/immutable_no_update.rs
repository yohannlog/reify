//! Compile-fail test: immutable tables should not have update() method.

use reify::Table;

#[derive(Table, Debug, Clone)]
#[table(name = "audit_log", immutable)]
pub struct AuditLog {
    #[column(primary_key, auto_increment)]
    pub id: i64,
    pub event: String,
}

fn main() {
    // This should fail to compile: update() is not generated for immutable tables
    let _ = AuditLog::update();
}
