use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use reify_core::db::{DbError, FromRow, Row};
use reify_core::value::Value;
use std::hint::black_box;

// ── Row5 — 5 columns ────────────────────────────────────────────────

#[allow(dead_code)] // bench scaffolding — fields exist to make Row5 a realistic struct shape
struct Row5 {
    col_0: i64,
    col_1: String,
    col_2: i64,
    col_3: String,
    col_4: i64,
}

impl FromRow for Row5 {
    fn from_row(row: &Row) -> Result<Self, DbError> {
        Ok(Row5 {
            col_0: match row.get("col_0") {
                Some(Value::I64(v)) => *v,
                _ => return Err(DbError::Conversion("col_0".into())),
            },
            col_1: match row.get("col_1") {
                Some(Value::String(v)) => v.clone(),
                _ => return Err(DbError::Conversion("col_1".into())),
            },
            col_2: match row.get("col_2") {
                Some(Value::I64(v)) => *v,
                _ => return Err(DbError::Conversion("col_2".into())),
            },
            col_3: match row.get("col_3") {
                Some(Value::String(v)) => v.clone(),
                _ => return Err(DbError::Conversion("col_3".into())),
            },
            col_4: match row.get("col_4") {
                Some(Value::I64(v)) => *v,
                _ => return Err(DbError::Conversion("col_4".into())),
            },
        })
    }
}

// ── Row20 — 20 columns ───────────────────────────────────────────────

#[allow(dead_code)] // bench scaffolding: fields populated for realistic FromRow workload
struct Row20 {
    col_0: i64,
    col_1: String,
    col_2: i64,
    col_3: String,
    col_4: i64,
    col_5: i64,
    col_6: String,
    col_7: i64,
    col_8: String,
    col_9: i64,
    col_10: i64,
    col_11: String,
    col_12: i64,
    col_13: String,
    col_14: i64,
    col_15: i64,
    col_16: String,
    col_17: i64,
    col_18: String,
    col_19: i64,
}

impl FromRow for Row20 {
    fn from_row(row: &Row) -> Result<Self, DbError> {
        macro_rules! get_i64 {
            ($col:literal) => {
                match row.get($col) {
                    Some(Value::I64(v)) => *v,
                    _ => return Err(DbError::Conversion($col.into())),
                }
            };
        }
        macro_rules! get_str {
            ($col:literal) => {
                match row.get($col) {
                    Some(Value::String(v)) => v.clone(),
                    _ => return Err(DbError::Conversion($col.into())),
                }
            };
        }
        Ok(Row20 {
            col_0: get_i64!("col_0"),
            col_1: get_str!("col_1"),
            col_2: get_i64!("col_2"),
            col_3: get_str!("col_3"),
            col_4: get_i64!("col_4"),
            col_5: get_i64!("col_5"),
            col_6: get_str!("col_6"),
            col_7: get_i64!("col_7"),
            col_8: get_str!("col_8"),
            col_9: get_i64!("col_9"),
            col_10: get_i64!("col_10"),
            col_11: get_str!("col_11"),
            col_12: get_i64!("col_12"),
            col_13: get_str!("col_13"),
            col_14: get_i64!("col_14"),
            col_15: get_i64!("col_15"),
            col_16: get_str!("col_16"),
            col_17: get_i64!("col_17"),
            col_18: get_str!("col_18"),
            col_19: get_i64!("col_19"),
        })
    }
}

// ── Row50 — 50 columns ───────────────────────────────────────────────

#[allow(dead_code)] // bench scaffolding: fields populated for realistic FromRow workload
struct Row50 {
    col_0: i64,
    col_1: String,
    col_2: i64,
    col_3: String,
    col_4: i64,
    col_5: i64,
    col_6: String,
    col_7: i64,
    col_8: String,
    col_9: i64,
    col_10: i64,
    col_11: String,
    col_12: i64,
    col_13: String,
    col_14: i64,
    col_15: i64,
    col_16: String,
    col_17: i64,
    col_18: String,
    col_19: i64,
    col_20: i64,
    col_21: String,
    col_22: i64,
    col_23: String,
    col_24: i64,
    col_25: i64,
    col_26: String,
    col_27: i64,
    col_28: String,
    col_29: i64,
    col_30: i64,
    col_31: String,
    col_32: i64,
    col_33: String,
    col_34: i64,
    col_35: i64,
    col_36: String,
    col_37: i64,
    col_38: String,
    col_39: i64,
    col_40: i64,
    col_41: String,
    col_42: i64,
    col_43: String,
    col_44: i64,
    col_45: i64,
    col_46: String,
    col_47: i64,
    col_48: String,
    col_49: i64,
}

impl FromRow for Row50 {
    fn from_row(row: &Row) -> Result<Self, DbError> {
        macro_rules! get_i64 {
            ($col:literal) => {
                match row.get($col) {
                    Some(Value::I64(v)) => *v,
                    _ => return Err(DbError::Conversion($col.into())),
                }
            };
        }
        macro_rules! get_str {
            ($col:literal) => {
                match row.get($col) {
                    Some(Value::String(v)) => v.clone(),
                    _ => return Err(DbError::Conversion($col.into())),
                }
            };
        }
        Ok(Row50 {
            col_0: get_i64!("col_0"),
            col_1: get_str!("col_1"),
            col_2: get_i64!("col_2"),
            col_3: get_str!("col_3"),
            col_4: get_i64!("col_4"),
            col_5: get_i64!("col_5"),
            col_6: get_str!("col_6"),
            col_7: get_i64!("col_7"),
            col_8: get_str!("col_8"),
            col_9: get_i64!("col_9"),
            col_10: get_i64!("col_10"),
            col_11: get_str!("col_11"),
            col_12: get_i64!("col_12"),
            col_13: get_str!("col_13"),
            col_14: get_i64!("col_14"),
            col_15: get_i64!("col_15"),
            col_16: get_str!("col_16"),
            col_17: get_i64!("col_17"),
            col_18: get_str!("col_18"),
            col_19: get_i64!("col_19"),
            col_20: get_i64!("col_20"),
            col_21: get_str!("col_21"),
            col_22: get_i64!("col_22"),
            col_23: get_str!("col_23"),
            col_24: get_i64!("col_24"),
            col_25: get_i64!("col_25"),
            col_26: get_str!("col_26"),
            col_27: get_i64!("col_27"),
            col_28: get_str!("col_28"),
            col_29: get_i64!("col_29"),
            col_30: get_i64!("col_30"),
            col_31: get_str!("col_31"),
            col_32: get_i64!("col_32"),
            col_33: get_str!("col_33"),
            col_34: get_i64!("col_34"),
            col_35: get_i64!("col_35"),
            col_36: get_str!("col_36"),
            col_37: get_i64!("col_37"),
            col_38: get_str!("col_38"),
            col_39: get_i64!("col_39"),
            col_40: get_i64!("col_40"),
            col_41: get_str!("col_41"),
            col_42: get_i64!("col_42"),
            col_43: get_str!("col_43"),
            col_44: get_i64!("col_44"),
            col_45: get_i64!("col_45"),
            col_46: get_str!("col_46"),
            col_47: get_i64!("col_47"),
            col_48: get_str!("col_48"),
            col_49: get_i64!("col_49"),
        })
    }
}

// ── Fixture builders ─────────────────────────────────────────────────

fn make_row(n: usize) -> Row {
    let mut columns = Vec::with_capacity(n);
    let mut values = Vec::with_capacity(n);
    for i in 0..n {
        columns.push(format!("col_{i}"));
        if i % 2 == 0 {
            values.push(Value::I64(i as i64));
        } else {
            values.push(Value::String(format!("val_{i}")));
        }
    }
    Row::new(columns, values)
}

// ── Benchmarks ───────────────────────────────────────────────────────

fn bench_from_row(c: &mut Criterion) {
    let row5 = make_row(5);
    let row20 = make_row(20);
    let row50 = make_row(50);

    let mut group = c.benchmark_group("from_row");

    group.bench_with_input(BenchmarkId::new("Row5", 5), &row5, |b, row| {
        b.iter(|| black_box(Row5::from_row(row)))
    });

    group.bench_with_input(BenchmarkId::new("Row20", 20), &row20, |b, row| {
        b.iter(|| black_box(Row20::from_row(row)))
    });

    group.bench_with_input(BenchmarkId::new("Row50", 50), &row50, |b, row| {
        b.iter(|| black_box(Row50::from_row(row)))
    });

    group.finish();
}

criterion_group!(benches, bench_from_row);
criterion_main!(benches);
