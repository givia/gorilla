use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use gorilla::{DataPoint, Decoder, Encoder};

/// Generate a realistic time-series dataset: constant 60s interval, slowly varying values.
fn generate_data(n: usize) -> Vec<DataPoint> {
    (0..n)
        .map(|i| {
            let t = 1_609_459_200 + (i as u64) * 60;
            let v = 20.0 + 5.0 * ((i as f64) * 0.01).sin() + (i as f64) * 0.001;
            DataPoint::new(t, v)
        })
        .collect()
}

/// Generate a dataset where every value is identical (best-case compression).
fn generate_constant_data(n: usize) -> Vec<DataPoint> {
    (0..n)
        .map(|i| DataPoint::new(1_609_459_200 + (i as u64) * 60, 42.0))
        .collect()
}

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");

    for size in [100, 1_000, 10_000, 100_000] {
        let data = generate_data(size);
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("varying", size), &data, |b, data| {
            b.iter(|| {
                let mut enc = Encoder::new();
                for dp in data {
                    enc.encode(black_box(*dp)).unwrap();
                }
                enc.finish().unwrap();
                black_box(enc.into_compressed())
            });
        });
    }

    for size in [100, 1_000, 10_000, 100_000] {
        let data = generate_constant_data(size);
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("constant", size), &data, |b, data| {
            b.iter(|| {
                let mut enc = Encoder::new();
                for dp in data {
                    enc.encode(black_box(*dp)).unwrap();
                }
                enc.finish().unwrap();
                black_box(enc.into_compressed())
            });
        });
    }

    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode");

    for size in [100, 1_000, 10_000, 100_000] {
        let data = generate_data(size);
        let mut enc = Encoder::new();
        for dp in &data {
            enc.encode(*dp).unwrap();
        }
        enc.finish().unwrap();
        let block = enc.into_compressed();

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("varying", size), &block, |b, block| {
            b.iter(|| {
                let points = Decoder::decode(black_box(block)).unwrap();
                black_box(points)
            });
        });
    }

    for size in [100, 1_000, 10_000, 100_000] {
        let data = generate_constant_data(size);
        let mut enc = Encoder::new();
        for dp in &data {
            enc.encode(*dp).unwrap();
        }
        enc.finish().unwrap();
        let block = enc.into_compressed();

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("constant", size), &block, |b, block| {
            b.iter(|| {
                let points = Decoder::decode(black_box(block)).unwrap();
                black_box(points)
            });
        });
    }

    group.finish();
}

fn bench_decode_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_iter");

    for size in [1_000, 10_000, 100_000] {
        let data = generate_data(size);
        let mut enc = Encoder::new();
        for dp in &data {
            enc.encode(*dp).unwrap();
        }
        enc.finish().unwrap();
        let block = enc.into_compressed();

        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("varying", size), &block, |b, block| {
            b.iter(|| {
                let count = Decoder::iter(black_box(block)).count();
                black_box(count)
            });
        });
    }

    group.finish();
}

fn bench_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("roundtrip");

    for size in [1_000, 10_000, 100_000] {
        let data = generate_data(size);
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("varying", size), &data, |b, data| {
            b.iter(|| {
                let mut enc = Encoder::new();
                for dp in data {
                    enc.encode(black_box(*dp)).unwrap();
                }
                enc.finish().unwrap();
                let block = enc.into_compressed();
                let points = Decoder::decode(&block).unwrap();
                black_box(points)
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode, bench_decode_iter, bench_roundtrip);
criterion_main!(benches);
