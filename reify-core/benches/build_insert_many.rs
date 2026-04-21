use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use reify_core::InsertManyBuilder;
use reify_core::schema::{ColumnDef, IndexDef};
use reify_core::table::Table;
use reify_core::value::Value;

struct BenchRow {
    id: i64,
    name: String,
    email: String,
    score: i32,
    active: bool,
}

impl Table for BenchRow {
    fn table_name() -> &'static str {
        "bench_rows"
    }

    fn column_names() -> &'static [&'static str] {
        &["id", "name", "email", "score", "active"]
    }

    fn as_values(&self) -> Vec<Value> {
        vec![
            Value::I64(self.id),
            Value::String(self.name.clone()),
            Value::String(self.email.clone()),
            Value::I32(self.score),
            Value::Bool(self.active),
        ]
    }

    fn column_defs() -> Vec<ColumnDef> {
        Vec::new()
    }

    fn indexes() -> Vec<IndexDef> {
        Vec::new()
    }
}

fn make_row(i: usize) -> BenchRow {
    BenchRow {
        id: i as i64,
        name: format!("user_{i}"),
        email: format!("user_{i}@example.com"),
        score: (i % 100) as i32,
        active: i % 2 == 0,
    }
}

fn bench_build_insert_many(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_insert_many");

    for n_rows in [1usize, 100, 10_000] {
        let rows: Vec<BenchRow> = (0..n_rows).map(make_row).collect();

        if n_rows == 10_000 {
            group.sample_size(10);
        }

        group.bench_with_input(BenchmarkId::from_parameter(n_rows), &rows, |b, rows| {
            b.iter(|| black_box(InsertManyBuilder::new(rows).build()))
        });
    }

    group.finish();
}

criterion_group!(benches, bench_build_insert_many);
criterion_main!(benches);
