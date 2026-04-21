use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

/// Fold-based implementation
fn bytecount_fold(bytes: &[u8]) -> usize {
    bytes.iter().filter(|&&b| b == b'?').count()
}

/// For-loop implementation
fn bytecount_loop(bytes: &[u8]) -> usize {
    let mut count = 0;
    for &b in bytes {
        count += (b == b'?') as usize;
    }
    count
}

fn bench_bytecount(c: &mut Criterion) {
    let mut group = c.benchmark_group("bytecount_question_marks");

    // Test avec différentes tailles de données
    for size in [64, 256, 1024, 4096, 16384, 65536] {
        // Génère des données avec ~10% de '?'
        let data: Vec<u8> = (0..size)
            .map(|i| if i % 10 == 0 { b'?' } else { b'a' })
            .collect();

        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("fold", size), &data, |b, data| {
            b.iter(|| bytecount_fold(black_box(data)))
        });

        group.bench_with_input(BenchmarkId::new("loop", size), &data, |b, data| {
            b.iter(|| bytecount_loop(black_box(data)))
        });
    }

    group.finish();
}

criterion_group!(benches, bench_bytecount);
criterion_main!(benches);
