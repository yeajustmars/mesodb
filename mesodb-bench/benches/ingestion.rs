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

    // Set memtable capacity high so we strictly isolate
    // transaction indexing speed without mixing compaction steps.
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
            cas_old_v: None,
        });
        facts.push(Fact {
            e,
            ident: ":sensor/reading".into(),
            v: Value::Float64(42.5 + (i as f64 * 0.1)),
            op: true,
            cas_old_v: None,
        });
    }
    facts
}

fn bench_ingestion(c: &mut Criterion) {
    let mut group = c.benchmark_group("Heavy Ingestion (Batches of 1000)");

    // 1. Strict Mode (Fsync every transaction)
    group.bench_function("WalSyncMode::Strict", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter();

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
        let _guard = rt.enter();

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

    // 3. Bitemporal Overwrites (Timeline Mutation / Contention)
    group.bench_function("Bitemporal Overwrites", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter();

        let dir = tempfile::tempdir().unwrap();
        let db = setup_db(&dir, WalSyncMode::Background);

        // Seed the 1,000 entities first
        rt.block_on(async {
            let initial_facts = generate_facts(1000, 0);
            db.transact(initial_facts).await.unwrap();
        });

        let mut loop_counter = 0f64;
        b.to_async(&rt).iter(|| {
            loop_counter += 1.0;

            // Re-asserting updates onto the existing 1,000 entities.
            // This isolates history timeline slicing performance.
            let mut update_facts = Vec::with_capacity(1000);
            for i in 0..1000 {
                update_facts.push(Fact {
                    e: i as u64,
                    ident: ":sensor/reading".into(),
                    v: Value::Float64(50.0 + loop_counter + (i as f64 * 0.1)),
                    op: true,
                    cas_old_v: None,
                });
            }

            async {
                db.transact(update_facts).await.unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_ingestion);
criterion_main!(benches);
