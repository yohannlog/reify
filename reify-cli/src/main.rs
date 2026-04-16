//! reify-cli — migration lifecycle management for Reify ORM.
//!
//! Commands:
//!   reify migrate                        — apply all pending migrations
//!   reify migrate --dry-run              — preview without applying
//!   reify migrate --since <date>         — (re-)apply migrations applied at or after <date>
//!   reify status                         — list applied/pending migrations
//!   reify new <name>                     — generate a migration file
//!   reify rollback                       — roll back the last migration
//!   reify rollback --to <version>        — roll back to a specific version
//!   reify rollback --since <date>        — roll back all migrations applied at or after <date>
//!
//! All commands that connect to the database require a connection URL, provided
//! via `--database-url` or the `DATABASE_URL` environment variable.
//!
//! URL format:
//!   postgres://user:pass@host/dbname
//!   mysql://user:pass@host/dbname
//!   sqlite:./path/to/db.sqlite   (or  sqlite::memory:)

use clap::{Parser, Subcommand};
use reify_core::migration::{
    MigrationRunner, SchemaDiff, generate_migration_file, generate_view_migration_file,
};

// ── CLI definition ───────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "reify",
    about = "Reify ORM — migration lifecycle management",
    version
)]
struct Cli {
    /// Database connection URL (overrides DATABASE_URL env var).
    ///
    /// Supported schemes: postgres://, mysql://, sqlite:
    #[arg(long, global = true, env = "DATABASE_URL")]
    database_url: Option<String>,

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
        /// Only (re-)apply migrations whose applied_at is at or after this date.
        /// Format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
        #[arg(long)]
        since: Option<String>,
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
        /// Generate a view migration template instead of a table migration.
        #[arg(long)]
        view: bool,
    },
    /// Roll back the last applied migration, to a specific version, or since a date.
    Rollback {
        /// Roll back to this version (inclusive).
        #[arg(long)]
        to: Option<String>,
        /// Roll back all migrations applied at or after this date (newest first).
        /// Format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
        #[arg(long)]
        since: Option<String>,
    },
}

// ── Entry point ──────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result = match &cli.command {
        Commands::Migrate { dry_run, since } => {
            let url = require_database_url(&cli.database_url);
            cmd_migrate(&url, *dry_run, since.as_deref()).await
        }
        Commands::Status => {
            let url = require_database_url(&cli.database_url);
            cmd_status(&url).await
        }
        Commands::New { name, dir, view } => {
            cmd_new(name, dir, *view);
            return;
        }
        Commands::Rollback { to, since } => {
            let url = require_database_url(&cli.database_url);
            cmd_rollback(&url, to.as_deref(), since.as_deref()).await
        }
    };

    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

// ── Database connection ──────────────────────────────────────────────

fn require_database_url(url: &Option<String>) -> String {
    match url {
        Some(u) => u.clone(),
        None => {
            eprintln!("error: no database URL provided.");
            eprintln!("Set DATABASE_URL or pass --database-url <URL>.");
            eprintln!();
            eprintln!("Examples:");
            eprintln!("  postgres://user:pass@localhost/mydb");
            eprintln!("  mysql://user:pass@localhost/mydb");
            eprintln!("  sqlite:./mydb.sqlite");
            std::process::exit(1);
        }
    }
}

/// Connect to the database described by `url` and run `f` with a boxed
/// `DynDatabase` handle. Driver is selected by URL scheme at runtime.
async fn with_db<F, Fut>(url: &str, f: F) -> Result<(), String>
where
    F: FnOnce(Box<dyn reify_core::db::DynDatabase>) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        connect_postgres(url, f).await
    } else if url.starts_with("mysql://") || url.starts_with("mariadb://") {
        connect_mysql(url, f).await
    } else if url.starts_with("sqlite:") {
        connect_sqlite(url, f).await
    } else {
        Err(format!(
            "unsupported database URL scheme in '{url}'.\n\
             Supported: postgres://, mysql://, sqlite:"
        ))
    }
}

#[cfg(feature = "postgres")]
async fn connect_postgres<F, Fut>(url: &str, f: F) -> Result<(), String>
where
    F: FnOnce(Box<dyn reify_core::db::DynDatabase>) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    use reify_postgres::{DpConfig, NoTls, PostgresDb};

    let pg_cfg = url
        .parse::<tokio_postgres::Config>()
        .map_err(|e| format!("invalid postgres URL: {e}"))?;

    let mut dp_cfg = DpConfig::new();
    dp_cfg.host = pg_cfg.get_hosts().first().and_then(|h| match h {
        tokio_postgres::config::Host::Tcp(s) => Some(s.clone()),
        _ => None,
    });
    dp_cfg.port = pg_cfg.get_ports().first().copied();
    dp_cfg.user = pg_cfg.get_user().map(str::to_string);
    dp_cfg.password = pg_cfg
        .get_password()
        .map(|b| String::from_utf8_lossy(b).into_owned());
    dp_cfg.dbname = pg_cfg.get_dbname().map(str::to_string);

    let db = PostgresDb::connect(dp_cfg, NoTls)
        .await
        .map_err(|e| format!("postgres connection failed: {e}"))?;

    f(Box::new(db)).await
}

#[cfg(not(feature = "postgres"))]
async fn connect_postgres<F, Fut>(_url: &str, _f: F) -> Result<(), String>
where
    F: FnOnce(Box<dyn reify_core::db::DynDatabase>) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    Err("reify-cli was compiled without the 'postgres' feature".into())
}

#[cfg(feature = "mysql")]
async fn connect_mysql<F, Fut>(url: &str, f: F) -> Result<(), String>
where
    F: FnOnce(Box<dyn reify_core::db::DynDatabase>) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    use reify_mysql::{MysqlDb, Opts};

    let opts = url
        .parse::<Opts>()
        .map_err(|e| format!("invalid mysql URL: {e}"))?;
    let db = MysqlDb::connect(opts)
        .await
        .map_err(|e| format!("mysql connection failed: {e}"))?;

    f(Box::new(db)).await
}

#[cfg(not(feature = "mysql"))]
async fn connect_mysql<F, Fut>(_url: &str, _f: F) -> Result<(), String>
where
    F: FnOnce(Box<dyn reify_core::db::DynDatabase>) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    Err("reify-cli was compiled without the 'mysql' feature".into())
}

#[cfg(feature = "sqlite")]
async fn connect_sqlite<F, Fut>(url: &str, f: F) -> Result<(), String>
where
    F: FnOnce(Box<dyn reify_core::db::DynDatabase>) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    use reify_sqlite::SqliteDb;

    // Strip the "sqlite:" scheme prefix
    let path = url.strip_prefix("sqlite:").unwrap_or(url);
    let db = if path == ":memory:" || path.is_empty() {
        SqliteDb::open_in_memory()
    } else {
        SqliteDb::open(path)
    }
    .map_err(|e| format!("sqlite open failed: {e}"))?;

    f(Box::new(db)).await
}

#[cfg(not(feature = "sqlite"))]
async fn connect_sqlite<F, Fut>(_url: &str, _f: F) -> Result<(), String>
where
    F: FnOnce(Box<dyn reify_core::db::DynDatabase>) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    Err("reify-cli was compiled without the 'sqlite' feature".into())
}

// ── Command implementations ──────────────────────────────────────────

/// `reify migrate [--dry-run] [--since <date>]`
async fn cmd_migrate(url: &str, dry_run: bool, since: Option<&str>) -> Result<(), String> {
    with_db(url, |db: Box<dyn reify_core::db::DynDatabase>| async move {
        let runner = MigrationRunner::new();
        if dry_run {
            let plans = runner.dry_run(&db).await.map_err(|e| e.to_string())?;
            if plans.is_empty() {
                println!("✓ No pending migrations.");
            } else {
                println!("DRY RUN — nothing will be written\n");
                // Display the global schema diff summary in the header.
                let global_diff = runner.diff(&db).await.map_err(|e| e.to_string())?;
                if !global_diff.is_empty() {
                    print!("{}", global_diff.display());
                    println!();
                }
                for plan in &plans {
                    print!("{}", plan.display());
                }
            }
        } else if let Some(since) = since {
            runner
                .run_since(&db, since)
                .await
                .map_err(|e| e.to_string())?;
            println!("✓ Migrations applied (since {since}).");
        } else {
            runner.run(&db).await.map_err(|e| e.to_string())?;
            println!("✓ Migrations applied.");
        }
        Ok(())
    })
    .await
}

/// `reify status`
async fn cmd_status(url: &str) -> Result<(), String> {
    with_db(url, |db: Box<dyn reify_core::db::DynDatabase>| async move {
        let runner = MigrationRunner::new();
        let statuses = runner.status(&db).await.map_err(|e| e.to_string())?;

        if statuses.is_empty() {
            println!("No migrations registered.");
        } else {
            for s in &statuses {
                println!("{}", s.display());
            }
        }
        Ok(())
    })
    .await
}

/// `reify new <name> [--view]`
fn cmd_new(name: &str, dir: &str, view: bool) {
    use std::fs;
    use std::path::Path;

    // Generate a timestamp-based version string
    let now = chrono::Utc::now();
    let version = format!("{}_000001_{name}", now.format("%Y%m%d"));

    let content = if view {
        generate_view_migration_file(name, &version)
    } else {
        generate_migration_file(name, &version)
    };
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
            if view {
                println!("Register it in your runner:");
                println!(
                    "  .add_view::<{struct_name}>()",
                    struct_name = to_pascal_case(name)
                );
            } else {
                println!("Register it in your runner:");
                println!("  .add({struct_name})", struct_name = to_pascal_case(name));
            }
        }
        Err(e) => {
            eprintln!("error: could not write '{}': {e}", path.display());
            std::process::exit(1);
        }
    }
}

/// `reify rollback [--to <version>] [--since <date>]`
async fn cmd_rollback(url: &str, to: Option<&str>, since: Option<&str>) -> Result<(), String> {
    if to.is_some() && since.is_some() {
        return Err("--to and --since are mutually exclusive".into());
    }
    with_db(url, |db: Box<dyn reify_core::db::DynDatabase>| async move {
        let runner = MigrationRunner::new();
        if let Some(version) = to {
            runner
                .rollback_to(&db, version)
                .await
                .map_err(|e| e.to_string())?;
        } else if let Some(since) = since {
            runner
                .rollback_since(&db, since)
                .await
                .map_err(|e| e.to_string())?;
        } else {
            runner.rollback(&db).await.map_err(|e| e.to_string())?;
        }
        println!("✓ Rollback complete.");
        Ok(())
    })
    .await
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
        cmd_new("test_migration", dir_str, false);
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
