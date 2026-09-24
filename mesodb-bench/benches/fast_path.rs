// mesodb-bench/benches/fast_path.rs

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::sync::Arc;
use tempfile::TempDir;

use mesodb_core::bitmap::BitmapStore;
use mesodb_core::config::{Config, WalSyncMode};
use mesodb_core::db::MesoDB;
use mesodb_core::schema::{SchemaMap, ValueType};
use mesodb_core::transactor::Fact;
use mesodb_core::types::Value;

fn setup_fast_path_db(dir: &TempDir, num_entities: usize) -> Arc<MesoDB> {
    let mut schema = SchemaMap::new();
    schema.add_attribute(":user/name", ValueType::String, false);
    schema.add_attribute(":user/age", ValueType::Int64, false);
    schema.add_attribute(":user/status", ValueType::String, false);

    let mut config = Config::default();
    config.storage.wal_sync_mode = WalSyncMode::Background;
    config.storage.memtable_max_rows = 10_000;

    // CRITICAL FIX: Set to 1 so every 10k batch forces RAM eviction and clears the dirty set
    config.compactor.backpressure_threshold = 1;

    let db = Arc::new(MesoDB::open(dir.path().join("fast_path.db"), schema, config).unwrap());

    let mut facts = Vec::with_capacity(num_entities * 3);
    for i in 1..=num_entities {
        let e = i as u64;
        facts.push(Fact {
            e,
            ident: ":user/name".into(),
            v: Value::String(format!("User_{}", i)),
            op: true,
            cas_old_v: None,
            valid_time: None,
        });
        facts.push(Fact {
            e,
            ident: ":user/age".into(),
            v: Value::Int64(20 + (i % 60) as i64),
            op: true,
            cas_old_v: None,
            valid_time: None,
        });
        facts.push(Fact {
            e,
            ident: ":user/status".into(),
            v: Value::String(if i % 2 == 0 { "active" } else { "inactive" }.into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        });
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        for chunk in facts.chunks(10_000) {
            db.transact(chunk.to_vec()).await.unwrap();
        }

        // Wait for the background worker to finish compaction and RAM eviction
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    });

    db
}

fn bench_fast_path(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    let mut group = c.benchmark_group("Tier 1 & Tier 2 Fast Path Engine");

    let dir = tempfile::tempdir().unwrap();
    let db = setup_fast_path_db(&dir, 10_000);

    // 1. Tier 1 Fast-Path Single Entity Pull (O(1) Lock-Free B-Tree Read)
    group.bench_function("Tier 1/Fast-Path Single Entity Pull", |b| {
        let query = r#"[:find (pull ?e [:user/name :user/age]) :where [500 :user/name _]]"#;
        b.to_async(&rt).iter(|| async {
            let res = db.query_json(black_box(query)).await.unwrap();
            black_box(res);
        });
    });

    // 2. Tier 1 Fast-Path Multi-Attribute EAV Lookup
    group.bench_function("Tier 1/Fast-Path Multi-Attribute Lookup", |b| {
        let query = r#"[:find ?n ?a :where [500 :user/name ?n] [500 :user/age ?a]]"#;
        b.to_async(&rt).iter(|| async {
            let res = db.query(black_box(query)).await.unwrap();
            black_box(res);
        });
    });

    // 3. Tier 2 Bitmap Inverted Query Lookup
    group.bench_function("Tier 2/Bitmap Inverted Single Attribute", |b| {
        let query = r#"[:find ?e :where [?e :user/status "active"]]"#;
        b.to_async(&rt).iter(|| async {
            let res = db.query(black_box(query)).await.unwrap();
            black_box(res);
        });
    });

    // 4. Standalone Roaring Bitmap Log Store Operations (Direct Mmap Slice Fetch)
    group.bench_function("Tier 2/BitmapStore Direct Mmap Fetch", |b| {
        let store_dir = tempfile::tempdir().unwrap();
        let mut store = BitmapStore::open(store_dir.path().join("bitmaps.idx")).unwrap();

        for i in 1..=50_000 {
            store
                .put(100, &Value::String("active".into()), i, true)
                .unwrap();
        }
        store.flush().unwrap();
        let snap = store.snapshot();

        b.iter(|| {
            let treemap = snap
                .get(black_box(100), black_box(&Value::String("active".into())))
                .unwrap()
                .unwrap();
            black_box(treemap);
        });
    });

    // 5. Pattern 5: Bitmap Intersection Join (Zero-Copy Relational Join)
    group.bench_function(
        "Tier 1+2/Fast-Path Relational Join (Bitmap Intersection)",
        |b| {
            let query = r#"[:find ?n :where [?e :user/status "active"] [?e :user/name ?n]]"#;
            b.to_async(&rt).iter(|| async {
                let res = db.query(black_box(query)).await.unwrap();
                black_box(res);
            });
        },
    );

    // 6. Pattern 6: Entity ID Range Scan
    group.bench_function("Tier 1/Fast-Path Entity Range Scan", |b| {
        let query = r#"[:find ?n :where [?e :user/name ?n] [(>= ?e 1000)] [(<= ?e 2000)]]"#;
        b.to_async(&rt).iter(|| async {
            let res = db.query(black_box(query)).await.unwrap();
            black_box(res);
        });
    });

    // 7. Pattern 7: Value Range Scan (AvtIndex)
    group.bench_function("Tier 3/Fast-Path Value Range Scan", |b| {
        let query = r#"[:find ?e :where [?e :user/age ?age] [(>= ?age 30)] [(<= ?age 40)]]"#;
        b.to_async(&rt).iter(|| async {
            let res = db.query(black_box(query)).await.unwrap();
            black_box(res);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_fast_path);
criterion_main!(benches);
