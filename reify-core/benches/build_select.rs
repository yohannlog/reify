use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use reify_core::condition::Condition;
use reify_core::query::SelectBuilder;
use reify_core::schema::{ColumnDef, IndexDef};
use reify_core::table::Table;
use reify_core::value::Value;

struct BenchTable;

impl Table for BenchTable {
    fn table_name() -> &'static str {
        "bench_table"
    }

    fn column_names() -> &'static [&'static str] {
        &["id", "name", "email", "score", "active"]
    }

    fn as_values(&self) -> Vec<Value> {
        vec![
            Value::I64(1),
            Value::String("name".into()),
            Value::String("email@example.com".into()),
            Value::I32(42),
            Value::Bool(true),
        ]
    }

    fn column_defs() -> Vec<ColumnDef> {
        Vec::new()
    }

    fn indexes() -> Vec<IndexDef> {
        Vec::new()
    }
}

fn bench_build_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_select");
    for n_conds in [0usize, 5, 20] {
        group.bench_with_input(BenchmarkId::from_parameter(n_conds), &n_conds, |b, &n| {
            b.iter(|| {
                let mut builder = SelectBuilder::<BenchTable>::new();
                for i in 0..n {
                    builder = builder.filter(Condition::Eq("score", Value::I32(i as i32)));
                }
                black_box(builder.build())
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_build_select);
criterion_main!(benches);
