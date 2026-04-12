//! reify-cli — migration lifecycle management for Reify ORM.
//!
//! Commands:
//!   reify migrate          — apply all pending migrations
//!   reify migrate --dry-run — preview without applying
//!   reify status           — list applied/pending migrations
//!   reify new <name>       — generate a migration file
//!   reify rollback         — roll back the last migration
//!   reify rollback --to <version> — roll back to a specific version

use clap::{Parser, Subcommand};
use reify_core::migration::{
    generate_migration_file, MigrationError, MigrationRunner, MigrationStatus,
};

// ── CLI definition ───────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "reify",
    about = "Reify ORM — migration lifecycle management",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Apply all pending migrations (use --dry-run to preview).
    Migrate {
        /// Preview SQL without applying it.
        #[arg(long)]
        dry_run: bool,
    },
    /// Show the status of all migrations (applied / pending).
    Status,
    /// Generate a new migration file.
    New {
        /// Snake_case name for the migration (e.g. add_user_city).
        name: String,
        /// Output directory for the generated file (default: migrations/).
        #[arg(long, default_value = "migrations")]
        dir: String,
    },
    /// Roll back the last applied migration (or to a specific version).
    Rollback {
        /// Roll back to this version (inclusive).
        #[arg(long)]
        to: Option<String>,
    },
}

// ── Entry point ──────────────────────────────────────────────────────

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Migrate { dry_run } => cmd_migrate(*dry_run),
        Commands::Status => cmd_status(),
        Commands::New { name, dir } => cmd_new(name, dir),
        Commands::Rollback { to } => cmd_rollback(to.as_deref()),
    }
}

// ── Command implementations ──────────────────────────────────────────

/// `reify migrate [--dry-run]`
///
/// In a real project the user wires their own `MigrationRunner` (with their
/// registered tables and migrations) and calls `.run()` / `.dry_run()` on it.
/// The CLI binary is a thin wrapper that prints instructions when run without
/// a project-specific runner configured.
fn cmd_migrate(dry_run: bool) {
    if dry_run {
        println!("┌─ DRY RUN — nothing will be written ──────────────────────────┐");
        println!("│ No runner configured. Wire your MigrationRunner in main.rs.  │");
        println!("│                                                               │");
        println!("│ Example:                                                      │");
        println!("│   MigrationRunner::new()                                      │");
        println!("│       .add_table::<User>()                                    │");
        println!("│       .add(MyMigration)                                       │");
        println!("│       .dry_run(&db).await?;                                   │");
        println!("└───────────────────────────────────────────────────────────────┘");
    } else {
        println!("reify migrate: apply your MigrationRunner in your project's main.");
        println!();
        println!("  MigrationRunner::new()");
        println!("      .add_table::<User>()");
        println!("      .run(&db).await?;");
    }
}

/// `reify status`
fn cmd_status() {
    println!("reify status: wire your MigrationRunner to query live status.");
    println!();
    println!("  let statuses = MigrationRunner::new()");
    println!("      .add_table::<User>()");
    println!("      .status(&db).await?;");
    println!();
    println!("  for s in statuses {{");
    println!("      println!(\"{{}}\", s.display());");
    println!("  }}");
}

/// `reify new <name>`
fn cmd_new(name: &str, dir: &str) {
    use std::fs;
    use std::path::Path;

    // Generate a timestamp-based version string
    let now = chrono::Utc::now();
    let version = format!(
        "{}_000001_{name}",
        now.format("%Y%m%d")
    );

    let content = generate_migration_file(name, &version);
    let filename = format!("{version}.rs");
    let path = Path::new(dir).join(&filename);

    // Create the output directory if it doesn't exist
    if let Err(e) = fs::create_dir_all(dir) {
        eprintln!("error: could not create directory '{dir}': {e}");
        std::process::exit(1);
    }

    match fs::write(&path, &content) {
        Ok(()) => {
            println!("Created: {}", path.display());
            println!();
            println!("Register it in your runner:");
            println!("  .add({struct_name})",
                struct_name = to_pascal_case(name));
        }
        Err(e) => {
            eprintln!("error: could not write '{}': {e}", path.display());
            std::process::exit(1);
        }
    }
}

/// `reify rollback [--to <version>]`
fn cmd_rollback(to: Option<&str>) {
    match to {
        Some(version) => {
            println!("reify rollback --to {version}: wire your runner:");
            println!();
            println!("  MigrationRunner::new()");
            println!("      .add(MyMigration)");
            println!("      .rollback_to(&db, \"{version}\").await?;");
        }
        None => {
            println!("reify rollback: wire your runner:");
            println!();
            println!("  MigrationRunner::new()");
            println!("      .add(MyMigration)");
            println!("      .rollback(&db).await?;");
        }
    }
}

// ── Utilities ────────────────────────────────────────────────────────

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_pascal_case_converts_snake_case() {
        assert_eq!(to_pascal_case("add_user_city"), "AddUserCity");
        assert_eq!(to_pascal_case("create_posts_table"), "CreatePostsTable");
        assert_eq!(to_pascal_case("simple"), "Simple");
    }

    #[test]
    fn cmd_new_generates_file_in_temp_dir() {
        let dir = std::env::temp_dir().join("reify_cli_test");
        let dir_str = dir.to_str().unwrap();
        cmd_new("test_migration", dir_str);
        // Check that at least one .rs file was created
        let entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "rs").unwrap_or(false))
            .collect();
        assert!(!entries.is_empty());
        // Clean up
        let _ = std::fs::remove_dir_all(&dir);
    }
}
