//! Runner: parses CLI args, dispatches scenarios across frameworks,
//! formats results.

use std::time::Duration;

use crate::scenarios;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Framework {
    Reify,
    Rusqlite,
    #[cfg(feature = "comparative")]
    Diesel,
    #[cfg(feature = "comparative")]
    SeaOrm,
    #[cfg(feature = "comparative")]
    Sqlx,
}

impl Framework {
    pub fn name(self) -> &'static str {
        match self {
            Framework::Reify => "reify",
            Framework::Rusqlite => "rusqlite",
            #[cfg(feature = "comparative")]
            Framework::Diesel => "diesel",
            #[cfg(feature = "comparative")]
            Framework::SeaOrm => "seaorm",
            #[cfg(feature = "comparative")]
            Framework::Sqlx => "sqlx",
        }
    }

    pub fn from_name(s: &str) -> Option<Framework> {
        match s {
            "reify" => Some(Framework::Reify),
            "rusqlite" => Some(Framework::Rusqlite),
            #[cfg(feature = "comparative")]
            "diesel" => Some(Framework::Diesel),
            #[cfg(feature = "comparative")]
            "seaorm" | "sea-orm" => Some(Framework::SeaOrm),
            #[cfg(feature = "comparative")]
            "sqlx" => Some(Framework::Sqlx),
            _ => None,
        }
    }

    pub fn all() -> Vec<Framework> {
        vec![
            Framework::Reify,
            Framework::Rusqlite,
            #[cfg(feature = "comparative")]
            Framework::Diesel,
            #[cfg(feature = "comparative")]
            Framework::SeaOrm,
            #[cfg(feature = "comparative")]
            Framework::Sqlx,
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scenario {
    Insert,
    InsertBatch,
    SelectAll,
    SelectByPk,
    Update,
    Delete,
}

impl Scenario {
    pub fn name(self) -> &'static str {
        match self {
            Scenario::Insert => "insert",
            Scenario::InsertBatch => "insert_batch",
            Scenario::SelectAll => "select_all",
            Scenario::SelectByPk => "select_by_pk",
            Scenario::Update => "update",
            Scenario::Delete => "delete",
        }
    }

    pub fn from_name(s: &str) -> Option<Scenario> {
        match s {
            "insert" => Some(Scenario::Insert),
            "insert_batch" => Some(Scenario::InsertBatch),
            "select_all" => Some(Scenario::SelectAll),
            "select_by_pk" => Some(Scenario::SelectByPk),
            "update" => Some(Scenario::Update),
            "delete" => Some(Scenario::Delete),
            _ => None,
        }
    }

    pub fn all() -> &'static [Scenario] {
        &[
            Scenario::Insert,
            Scenario::InsertBatch,
            Scenario::SelectAll,
            Scenario::SelectByPk,
            Scenario::Update,
            Scenario::Delete,
        ]
    }
}

#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub rows: usize,
    pub iters: usize,
    pub frameworks: Vec<Framework>,
    pub scenarios: Vec<Scenario>,
    pub json: bool,
    pub help: bool,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            rows: 1000,
            iters: 5,
            frameworks: Framework::all(),
            scenarios: Scenario::all().to_vec(),
            json: false,
            help: false,
        }
    }
}

impl BenchConfig {
    pub fn parse(args: &[String]) -> Result<Self, String> {
        let mut cfg = Self::default();
        let mut i = 0;
        while i < args.len() {
            let a = &args[i];
            match a.as_str() {
                "-h" | "--help" => {
                    cfg.help = true;
                    return Ok(cfg);
                }
                "--json" => {
                    cfg.json = true;
                    i += 1;
                }
                "--rows" => {
                    let v = args.get(i + 1).ok_or("missing value for --rows")?;
                    cfg.rows = v.parse().map_err(|_| "invalid --rows")?;
                    i += 2;
                }
                "--iters" => {
                    let v = args.get(i + 1).ok_or("missing value for --iters")?;
                    cfg.iters = v.parse().map_err(|_| "invalid --iters")?;
                    i += 2;
                }
                "--only" => {
                    let v = args.get(i + 1).ok_or("missing value for --only")?;
                    cfg.frameworks = v
                        .split(',')
                        .map(|s| {
                            Framework::from_name(s.trim())
                                .ok_or_else(|| format!("unknown framework '{s}'"))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    i += 2;
                }
                "--scenario" => {
                    let v = args.get(i + 1).ok_or("missing value for --scenario")?;
                    cfg.scenarios = vec![
                        Scenario::from_name(v.trim())
                            .ok_or_else(|| format!("unknown scenario '{v}'"))?,
                    ];
                    i += 2;
                }
                other => return Err(format!("unknown argument '{other}'")),
            }
        }
        Ok(cfg)
    }
}

#[derive(Debug, Clone)]
pub struct BenchResult {
    pub framework: Framework,
    pub scenario: Scenario,
    /// Median wall time across iterations.
    pub median: Duration,
    /// `None` when the framework is not compiled in.
    pub skipped: Option<&'static str>,
}

pub async fn run_all(cfg: &BenchConfig) -> Vec<BenchResult> {
    let mut out = Vec::new();
    for &scn in &cfg.scenarios {
        for &fw in &cfg.frameworks {
            out.push(run_one(cfg, fw, scn).await);
        }
    }
    out
}

async fn run_one(cfg: &BenchConfig, fw: Framework, scn: Scenario) -> BenchResult {
    match fw {
        Framework::Reify => BenchResult {
            framework: fw,
            scenario: scn,
            median: scenarios::reify::run(scn, cfg.rows, cfg.iters).await,
            skipped: None,
        },
        Framework::Rusqlite => BenchResult {
            framework: fw,
            scenario: scn,
            median: scenarios::rusqlite_raw::run(scn, cfg.rows, cfg.iters).await,
            skipped: None,
        },
        #[cfg(feature = "comparative")]
        Framework::Diesel => BenchResult {
            framework: fw,
            scenario: scn,
            median: scenarios::diesel::run(scn, cfg.rows, cfg.iters).await,
            skipped: None,
        },
        #[cfg(feature = "comparative")]
        Framework::SeaOrm => BenchResult {
            framework: fw,
            scenario: scn,
            median: scenarios::seaorm::run(scn, cfg.rows, cfg.iters).await,
            skipped: None,
        },
        #[cfg(feature = "comparative")]
        Framework::Sqlx => BenchResult {
            framework: fw,
            scenario: scn,
            median: scenarios::sqlx_raw::run(scn, cfg.rows, cfg.iters).await,
            skipped: None,
        },
    }
}

pub fn print_table(cfg: &BenchConfig, results: &[BenchResult]) {
    if cfg.json {
        print_json(cfg, results);
        return;
    }

    println!();
    println!(
        "Reify benchmark suite — rows={}, iters={}, backend=SQLite (in-memory)",
        cfg.rows, cfg.iters
    );
    #[cfg(not(feature = "comparative"))]
    println!(
        "note: comparative frameworks (diesel, seaorm, sqlx) disabled. \
         Rebuild with --features comparative."
    );
    println!();

    // Columns: scenario, then one per framework.
    let mut header = String::from("| scenario      ");
    let mut sep = String::from("|---------------");
    for &fw in &cfg.frameworks {
        header.push_str(&format!("| {:>12} ", fw.name()));
        sep.push_str("|-------------");
    }
    header.push('|');
    sep.push('|');
    println!("{header}");
    println!("{sep}");

    for &scn in &cfg.scenarios {
        let mut row = format!("| {:<13} ", scn.name());
        // find fastest for this scenario to mark with `*`
        let mut fastest: Option<Duration> = None;
        for &fw in &cfg.frameworks {
            if let Some(r) = results
                .iter()
                .find(|r| r.framework == fw && r.scenario == scn)
                && r.skipped.is_none()
            {
                fastest = Some(fastest.map(|f| f.min(r.median)).unwrap_or(r.median));
            }
        }
        for &fw in &cfg.frameworks {
            let cell = match results
                .iter()
                .find(|r| r.framework == fw && r.scenario == scn)
            {
                Some(r) if r.skipped.is_none() => {
                    let mark = if Some(r.median) == fastest { "*" } else { " " };
                    format!("{}{:>11}", mark, fmt_dur(r.median))
                }
                _ => "          — ".to_string(),
            };
            row.push_str(&format!("| {cell} "));
        }
        row.push('|');
        println!("{row}");
    }
    println!();
    println!("* = fastest for that scenario. Times are medians.");
}

fn print_json(_cfg: &BenchConfig, results: &[BenchResult]) {
    print!("[");
    for (i, r) in results.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!(
            "{{\"framework\":\"{}\",\"scenario\":\"{}\",\"median_ns\":{}}}",
            r.framework.name(),
            r.scenario.name(),
            r.median.as_nanos()
        );
    }
    println!("]");
}

fn fmt_dur(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.2} µs", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2} s", d.as_secs_f64())
    }
}
