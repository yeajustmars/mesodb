// mesodb-bench/benches/ingestion.rs

use criterion::{Criterion, criterion_group, criterion_main};
use tempfile::TempDir;

use mesodb_core::config::{Config, WalSyncMode};
use mesodb_core::db::MesoDB;
use mesodb_core::schema::{SchemaMap, ValueType};
use mesodb_core::transactor::Fact;
use mesodb_core::types::Value;

fn setup_db(dir: &TempDir, sync_mode: WalSyncMode) -> MesoDB {
    let mut schema = SchemaMap::new();
    schema.add_attribute(":sensor/id", ValueType::Int64, false);
    schema.add_attribute(":sensor/reading", ValueType::Float64, false);

    let mut config = Config::default();
    config.storage.wal_sync_mode = sync_mode;

    // We keep memtable rows high so we are strictly benchmarking
    // the WAL/Transactor overhead, not the Parquet compactor.
    config.storage.memtable_max_rows = 1_000_000;

    MesoDB::open(dir.path().join("bench.db"), schema, config).unwrap()
}

fn generate_facts(batch_size: usize, start_id: i64) -> Vec<Fact> {
    let mut facts = Vec::with_capacity(batch_size * 2);
    for i in 0..batch_size {
        let e = (start_id + i as i64) as u64;
        facts.push(Fact {
            e,
            ident: ":sensor/id".into(),
            v: Value::Int64(e as i64),
            op: true,
        });
        facts.push(Fact {
            e,
            ident: ":sensor/reading".into(),
            v: Value::Float64(42.5 + (i as f64 * 0.1)),
            op: true,
        });
    }
    facts
}

fn bench_ingestion(c: &mut Criterion) {
    let mut group = c.benchmark_group("Heavy Ingestion (Batches of 1000)");

    // 1. Strict Mode (Fsync every transaction)
    group.bench_function("WalSyncMode::Strict", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter(); // FIX: Provide tokio context so the background worker can spawn!

        let dir = tempfile::tempdir().unwrap();
        let db = setup_db(&dir, WalSyncMode::Strict);
        let mut tx_counter = 0;

        b.to_async(&rt).iter(|| {
            let facts = generate_facts(1000, tx_counter * 1000);
            tx_counter += 1;
            async {
                db.transact(facts).await.unwrap();
            }
        });
    });

    // 2. Background Mode (OS-managed sync)
    group.bench_function("WalSyncMode::Background", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter(); // FIX: Provide tokio context so the background worker can spawn!

        let dir = tempfile::tempdir().unwrap();
        let db = setup_db(&dir, WalSyncMode::Background);
        let mut tx_counter = 0;

        b.to_async(&rt).iter(|| {
            let facts = generate_facts(1000, tx_counter * 1000);
            tx_counter += 1;
            async {
                db.transact(facts).await.unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_ingestion);
criterion_main!(benches);
