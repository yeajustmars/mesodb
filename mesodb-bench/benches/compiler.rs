// mesodb-bench/benches/compiler.rs

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mesodb_core::edn::parse_transaction;
use std::hint::black_box;

fn generate_huge_edn_transaction(num_facts: usize) -> String {
    let mut edn = String::with_capacity(num_facts * 64);
    edn.push_str("[\n");
    for i in 0..num_facts {
        edn.push_str(&format!(
            "  [:db/add {} :sensor/reading {} #inst \"2026-07-17T23:00:00Z\"]\n",
            i,
            42.5 + (i as f64 * 0.1)
        ));
    }
    edn.push(']');
    edn
}

fn bench_edn_compiler(c: &mut Criterion) {
    let mut group = c.benchmark_group("Zero-Allocation EDN Compiler");

    let num_facts = 10_000;
    let edn_payload = generate_huge_edn_transaction(num_facts);

    // Register the total byte size so Criterion can report raw MB/s throughput
    group.throughput(Throughput::Bytes(edn_payload.len() as u64));

    group.bench_function("Parse 10,000 Facts", |b| {
        b.iter(|| {
            // black_box prevents the Rust compiler from optimizing away the parsing work
            let result = parse_transaction(black_box(&edn_payload)).unwrap();
            black_box(result);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_edn_compiler);
criterion_main!(benches);
