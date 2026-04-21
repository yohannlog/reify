use super::diff::SchemaDiff;
use sha2::{Digest, Sha256};

/// Compute a SHA-256 checksum over a list of SQL statements.
///
/// Statements are concatenated with `\n` separators before hashing,
/// producing a stable hex digest that changes if any statement is modified.
pub fn compute_checksum(statements: &[String]) -> String {
    let mut hasher = Sha256::new();
    for stmt in statements {
        hasher.update(stmt.as_bytes());
        hasher.update(b"\n");
    }
    format!("{:x}", hasher.finalize())
}

/// The result of a dry-run: what *would* be executed, without applying it.
#[derive(Debug, Clone)]
pub struct MigrationPlan {
    /// Migration version string.
    pub version: String,
    /// Human-readable description.
    pub description: String,
    /// Optional free-text comment (from `Migration::comment()`).
    pub comment: Option<String>,
    /// SQL statements that would be executed.
    pub statements: Vec<String>,
    /// SHA-256 hex digest of the concatenated SQL statements.
    pub checksum: String,
    /// Structural schema diff for auto-diff plans (None for manual/view plans).
    pub schema_diff: Option<SchemaDiff>,
    /// Maximum wall-clock time allowed for this migration's transaction.
    /// `None` means no timeout (the default for auto-diff and view plans).
    /// Set via `Migration::timeout()` on manual migrations.
    pub timeout: Option<std::time::Duration>,
}

impl MigrationPlan {
    /// Pretty-print the plan to a string (mirrors the dry-run output format).
    ///
    /// When `schema_diff` is present, the structural diff (✚/✖/⇄ per column)
    /// is displayed before the raw SQL statements.
    pub fn display(&self) -> String {
        let checksum_short = &self.checksum[..self.checksum.len().min(8)];
        let mut out = format!(
            "  ~ Would apply (up): {} [{}]\n",
            self.version, checksum_short
        );
        out.push_str(&format!("    -- {}\n", self.description));
        if let Some(c) = &self.comment {
            out.push_str(&format!("    -- comment: {c}\n"));
        }
        if let Some(diff) = &self.schema_diff {
            if !diff.is_empty() {
                // Indent each line of the diff block by 4 spaces.
                for line in diff.display().lines() {
                    out.push_str(&format!("    {line}\n"));
                }
            }
        }
        if let Some(t) = self.timeout {
            out.push_str(&format!("    -- timeout: {}s\n", t.as_secs()));
        }
        out.push_str("    SQL:\n");
        for stmt in &self.statements {
            for line in stmt.lines() {
                out.push_str(&format!("      {line}\n"));
            }
        }
        out
    }
}

impl std::fmt::Display for MigrationPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display())
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
    /// Timestamp when the migration was applied (`applied_at` column).
    /// `None` for pending migrations or when the column is unavailable.
    pub applied_at: Option<String>,
}

impl MigrationStatus {
    /// Format for CLI display.
    ///
    /// Output format:
    /// ```text
    /// ✓ Applied  [auto]    auto__users  (2024-03-20 12:00:00+00)
    /// ~ Pending  [manual]  20240320_000001_add_city
    /// ```
    pub fn display(&self) -> String {
        let mark = if self.applied {
            "✓ Applied"
        } else {
            "~ Pending"
        };
        let kind = if self.is_auto { "[auto]  " } else { "[manual]" };
        match &self.applied_at {
            Some(ts) => format!("  {mark}  {kind}  {}  ({})", self.version, ts),
            None => format!("  {mark}  {kind}  {}", self.version),
        }
    }
}

impl std::fmt::Display for MigrationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display())
    }
}
