// mesodb-bench/benches/velocity_sentinel.rs

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::time::Duration;
use tempfile::TempDir;

use mesodb_core::config::{Config, WalSyncMode};
use mesodb_core::db::{MesoDb, QueryOptions};
use mesodb_core::schema::{SchemaMap, ValueType};
use mesodb_core::transactor::Fact;
use mesodb_core::types::Value;

// =====================================================================
// VECTOR 4: VELOCITY SENTINEL BENCHMARKS
// =====================================================================

fn setup_scaling_db(dir: &TempDir, num_facts: usize) -> MesoDb {
    let mut schema = SchemaMap::new();
    schema.add_attribute(":sys/scale", ValueType::Int64, false);

    let mut config = Config::default();
    config.storage.wal_sync_mode = WalSyncMode::Background;

    // We set a high threshold so we are strictly benchmarking DataFusion's
    // raw execution velocity in memory, isolating it from disk I/O jitter.
    config.storage.memtable_max_rows = 1_500_000;

    let db = MesoDb::open(
        dir.path().join(format!("scale_{}.db", num_facts)),
        schema,
        config,
    )
    .unwrap();

    let mut facts = Vec::with_capacity(num_facts);
    for i in 0..num_facts {
        facts.push(Fact {
            e: i as u64,
            ident: ":sys/scale".into(),
            v: Value::Int64(i as i64),
            op: true,
            cas_old_v: None,
            valid_time: None,
        });
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Chunk the ingestion to avoid blowing up the Tokio runtime overhead
        for chunk in facts.chunks(100_000) {
            db.transact(chunk.to_vec()).await.unwrap();
        }
    });

    db
}

fn bench_velocity_sentinel(c: &mut Criterion) {
    let mut group = c.benchmark_group("Velocity Sentinel: Read Latency Scaling");

    // Configure Criterion to run slightly longer for these heavy data operations
    group.measurement_time(Duration::from_secs(10));

    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    // The Gauntlet Thresholds
    let scales = [10_000, 100_000, 1_000_000];

    for scale in scales {
        let dir = tempfile::tempdir().unwrap();
        let db = setup_scaling_db(&dir, scale);

        group.bench_with_input(
            BenchmarkId::new("Point-in-Time Aggregation", scale),
            &scale,
            |b, _scale| {
                // A full table scan aggregation to force DataFusion to process every single row
                let query = r#"[:find (count ?e) :where [?e :sys/scale ?v]]"#;
                b.to_async(&rt).iter(|| async {
                    db.query(query).await.unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Historical Audit Full Scan", scale),
            &scale,
            |b, _scale| {
                // Bypasses the masking node to pull the raw history trail
                let query = r#"[:find (count ?e) :where [?e :sys/scale ?v _ true]]"#;
                let opts = QueryOptions {
                    history: true,
                    ..Default::default()
                };
                b.to_async(&rt).iter(|| async {
                    db.query_with_options(query, opts.clone()).await.unwrap();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_velocity_sentinel);
criterion_main!(benches);
