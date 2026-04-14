/// The result of a dry-run: what *would* be executed, without applying it.
#[derive(Debug, Clone)]
pub struct MigrationPlan {
    /// Migration version string.
    pub version: String,
    /// Human-readable description.
    pub description: String,
    /// SQL statements that would be executed.
    pub statements: Vec<String>,
    /// Direction: `true` = up (apply), `false` = down (rollback).
    pub is_up: bool,
}

impl MigrationPlan {
    /// Pretty-print the plan to a string (mirrors the dry-run output format).
    pub fn display(&self) -> String {
        let dir = if self.is_up { "up" } else { "down" };
        let mut out = format!("  ~ Would apply ({dir}): {}\n", self.version);
        out.push_str(&format!("    -- {}\n", self.description));
        for stmt in &self.statements {
            for line in stmt.lines() {
                out.push_str(&format!("    {line}\n"));
            }
        }
        out
    }
}

/// Status of a single migration (applied or pending).
#[derive(Debug, Clone)]
pub struct MigrationStatus {
    /// Migration version string.
    pub version: String,
    /// Human-readable description.
    pub description: String,
    /// Whether this migration has been applied.
    pub applied: bool,
    /// Whether this is an auto-diff migration (vs. manual).
    pub is_auto: bool,
}

impl MigrationStatus {
    /// Format for CLI display.
    pub fn display(&self) -> String {
        let mark = if self.applied {
            "✓ Applied "
        } else {
            "~ Pending "
        };
        format!("  {mark}  {}", self.version)
    }
}
