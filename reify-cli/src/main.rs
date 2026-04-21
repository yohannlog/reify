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
//!   reify bench [-- <bench-args>]        — run comparative benchmarks (reify vs diesel/seaorm/sqlx)
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

// ── Exit codes ───────────────────────────────────────────────────────
//
// Follow a conventional scheme so CI pipelines and wrappers can react
// to specific failure classes without parsing stderr.
//
// | code | meaning                                                     |
// |------|-------------------------------------------------------------|
// | 0    | success                                                     |
// | 1    | generic runtime error (I/O, connection, driver, etc.)       |
// | 2    | usage error (missing URL, bad flags, bad path)              |
// | 3    | migration conflict (integrity, ordering, rollback refused)  |
// | 4    | connection error (DB unreachable, auth failure)             |
//
// Exit code constants keep call sites self-documenting.
const EXIT_GENERIC: i32 = 1;
const EXIT_USAGE: i32 = 2;
const EXIT_MIGRATION_CONFLICT: i32 = 3;
const EXIT_CONNECTION: i32 = 4;

/// Classified error with the exit code the CLI should use.
struct CliError {
    code: i32,
    message: String,
}

impl CliError {
    fn usage(msg: impl Into<String>) -> Self {
        Self {
            code: EXIT_USAGE,
            message: msg.into(),
        }
    }
}

/// Heuristic classification for errors surfaced by `reify_core::migration`.
/// Message prefixes are stable across the crate (see `MigrationError`).
fn classify_migration_error(msg: String) -> CliError {
    if let Some(stripped) = msg.strip_prefix("usage: ") {
        return CliError::usage(stripped.to_string());
    }
    let lower = msg.to_ascii_lowercase();
    let code = if lower.contains("conflict")
        || lower.contains("checksum")
        || lower.contains("already applied")
        || lower.contains("irreversible")
        || lower.contains("out of order")
    {
        EXIT_MIGRATION_CONFLICT
    } else if lower.contains("connection")
        || lower.contains("unreachable")
        || lower.contains("authentication")
        || lower.contains("auth failed")
        || lower.contains("could not connect")
    {
        EXIT_CONNECTION
    } else if lower.contains("invalid ") && lower.contains(" url") {
        EXIT_USAGE
    } else {
        EXIT_GENERIC
    };
    CliError { code, message: msg }
}

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
    /// Run the comparative benchmark suite.
    ///
    /// Shells out to `cargo run -p reify-bench --release`. Extra arguments
    /// are forwarded verbatim, e.g. `reify bench --rows 10000 --only reify`.
    /// Enable the comparative drivers with `--features comparative`.
    #[command(trailing_var_arg = true, allow_hyphen_values = true)]
    Bench {
        /// Enable the comparative suite (diesel, seaorm, sqlx).
        #[arg(long)]
        comparative: bool,
        /// Arguments forwarded to reify-bench.
        args: Vec<String>,
    },
}

// ── Entry point ──────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result: Result<(), CliError> = match &cli.command {
        Commands::Migrate { dry_run, since } => {
            let url = require_database_url(&cli.database_url);
            cmd_migrate(&url, *dry_run, since.as_deref())
                .await
                .map_err(classify_migration_error)
        }
        Commands::Status => {
            let url = require_database_url(&cli.database_url);
            cmd_status(&url).await.map_err(classify_migration_error)
        }
        Commands::New { name, dir, view } => {
            cmd_new(name, dir, *view);
            return;
        }
        Commands::Rollback { to, since } => {
            let url = require_database_url(&cli.database_url);
            cmd_rollback(&url, to.as_deref(), since.as_deref())
                .await
                .map_err(classify_migration_error)
        }
        Commands::Bench { comparative, args } => {
            let code = cmd_bench(*comparative, args);
            if code != 0 {
                std::process::exit(code);
            }
            return;
        }
    };

    if let Err(e) = result {
        eprintln!("error: {}", e.message);
        std::process::exit(e.code);
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
            std::process::exit(EXIT_USAGE);
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

    // Strip the "sqlite:" scheme prefix.
    let path = url.strip_prefix("sqlite:").unwrap_or(url);

    let db = if path == ":memory:" || path.is_empty() {
        SqliteDb::open_in_memory()
    } else {
        // Resolve the path safely before handing it to rusqlite.
        let resolved = resolve_sqlite_path(path)?;
        SqliteDb::open(&resolved)
    }
    .map_err(|e| format!("sqlite open failed: {e}"))?;

    f(Box::new(db)).await
}

/// Resolve a sqlite filesystem path.
///
/// - Always rejects raw `..` segments, which can be used to escape a chroot
///   or confuse operators (`sqlite:/var/db/../etc/shadow`).
/// - When `REIFY_CLI_RESTRICTIVE_PATHS=1`, additionally requires the file (or
///   its parent, if the file does not yet exist) to canonicalise successfully
///   — i.e. the path must resolve to a real location on disk with no symlink
///   games. This is intended for locked-down CI/CD environments.
///
/// Otherwise (the default), the path is accepted as-is after the `..` check
/// so that relative paths like `./mydb.sqlite` keep working when the file
/// does not yet exist.
fn resolve_sqlite_path(path: &str) -> Result<String, String> {
    use std::path::{Component, Path};

    let p = Path::new(path);
    for comp in p.components() {
        if matches!(comp, Component::ParentDir) {
            return Err(format!(
                "sqlite path '{path}' contains '..'; use an absolute path instead"
            ));
        }
    }

    let restrictive = std::env::var("REIFY_CLI_RESTRICTIVE_PATHS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    if restrictive {
        // Canonicalise the file if it exists, otherwise its parent directory.
        let canon = if p.exists() {
            p.canonicalize()
        } else {
            let parent = p.parent().filter(|pp| !pp.as_os_str().is_empty());
            match parent {
                Some(pp) => pp
                    .canonicalize()
                    .map(|cp| cp.join(p.file_name().unwrap_or_else(|| std::ffi::OsStr::new("")))),
                None => std::env::current_dir().map(|cwd| cwd.join(p)),
            }
        }
        .map_err(|e| format!("sqlite path '{path}' could not be canonicalised: {e}"))?;
        return Ok(canon.to_string_lossy().into_owned());
    }

    Ok(path.to_string())
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
        std::process::exit(EXIT_GENERIC);
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
            std::process::exit(EXIT_GENERIC);
        }
    }
}

/// `reify rollback [--to <version>] [--since <date>]`
async fn cmd_rollback(url: &str, to: Option<&str>, since: Option<&str>) -> Result<(), String> {
    if to.is_some() && since.is_some() {
        // Prefixed with "usage:" so `classify_migration_error` falls through
        // to the generic bucket and we hit usage below via an explicit check.
        return Err("usage: --to and --since are mutually exclusive".into());
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

// ── `reify bench` ────────────────────────────────────────────────────

/// Run the comparative benchmark suite by delegating to `cargo run -p reify-bench`.
///
/// This keeps the bench crate fully decoupled from the CLI binary: heavy
/// comparative dependencies (Diesel, SeaORM, sqlx) are only built on demand.
fn cmd_bench(comparative: bool, extra: &[String]) -> i32 {
    use std::process::Command;

    let mut cmd = Command::new("cargo");
    cmd.arg("run").arg("--release").arg("-p").arg("reify-bench");
    if comparative {
        cmd.arg("--features").arg("comparative");
    }
    cmd.arg("--");
    for a in extra {
        cmd.arg(a);
    }
    match cmd.status() {
        Ok(s) => s.code().unwrap_or(EXIT_GENERIC),
        Err(e) => {
            eprintln!("error: failed to invoke cargo: {e}");
            eprintln!(
                "note: `reify bench` requires the reify workspace checkout and cargo on PATH."
            );
            EXIT_GENERIC
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
    fn resolve_sqlite_path_rejects_parent_dir() {
        let err = resolve_sqlite_path("../secret.db").unwrap_err();
        assert!(err.contains(".."), "unexpected message: {err}");
    }

    #[test]
    fn resolve_sqlite_path_rejects_nested_parent_dir() {
        let err = resolve_sqlite_path("a/b/../c.db").unwrap_err();
        assert!(err.contains(".."), "unexpected message: {err}");
    }

    #[test]
    fn resolve_sqlite_path_passes_plain_relative_path() {
        // Ensure the restrictive flag is off so the path is returned as-is.
        // SAFETY: `set_var` is `unsafe` in Rust 2024; this is a single-threaded
        // test and the variable is only consulted on entry to `resolve_sqlite_path`.
        // See: https://doc.rust-lang.org/stable/std/env/fn.set_var.html
        unsafe {
            std::env::remove_var("REIFY_CLI_RESTRICTIVE_PATHS");
        }
        let resolved = resolve_sqlite_path("./mydb.sqlite").unwrap();
        assert_eq!(resolved, "./mydb.sqlite");
    }

    #[test]
    fn classify_migration_error_buckets() {
        assert_eq!(
            classify_migration_error("checksum mismatch on v1".into()).code,
            EXIT_MIGRATION_CONFLICT
        );
        assert_eq!(
            classify_migration_error("postgres connection failed".into()).code,
            EXIT_CONNECTION
        );
        assert_eq!(
            classify_migration_error("usage: bad flag".into()).code,
            EXIT_USAGE
        );
        assert_eq!(
            classify_migration_error("invalid postgres url".into()).code,
            EXIT_USAGE
        );
        assert_eq!(
            classify_migration_error("unexpected io error".into()).code,
            EXIT_GENERIC
        );
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
