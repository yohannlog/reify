use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use reify_core::query::rewrite_placeholders_pg;
use std::hint::black_box;

fn bench_rewrite_placeholders(c: &mut Criterion) {
    // short: 3 placeholders — single-row INSERT
    let short = "INSERT INTO t (a, b, c) VALUES (?, ?, ?)";

    // medium: 20 placeholders — 20-column SELECT with WHERE
    let medium = {
        let cols: Vec<String> = (1..=20).map(|i| format!("col{i}")).collect();
        let select_list = cols.join(", ");
        let where_clause: Vec<String> = (1..=20).map(|i| format!("col{i} = ?")).collect();
        format!(
            "SELECT {select_list} FROM t WHERE {}",
            where_clause.join(" AND ")
        )
    };

    // long: 200 placeholders — 10-column × 20-row batch INSERT
    let long = {
        let cols: Vec<&str> = vec!["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"];
        let col_list = cols.join(", ");
        let row_placeholder = format!("({})", ["?"; 10].join(", "));
        let rows: Vec<&str> = vec![row_placeholder.as_str(); 20];
        format!("INSERT INTO t ({col_list}) VALUES {}", rows.join(", "))
    };

    let mut group = c.benchmark_group("rewrite_placeholders");

    group.bench_with_input(BenchmarkId::new("short", 3), short, |b, sql| {
        b.iter(|| black_box(rewrite_placeholders_pg(sql)))
    });

    group.bench_with_input(BenchmarkId::new("medium", 20), medium.as_str(), |b, sql| {
        b.iter(|| black_box(rewrite_placeholders_pg(sql)))
    });

    group.bench_with_input(BenchmarkId::new("long", 200), long.as_str(), |b, sql| {
        b.iter(|| black_box(rewrite_placeholders_pg(sql)))
    });

    group.finish();
}

criterion_group!(benches, bench_rewrite_placeholders);
criterion_main!(benches);
