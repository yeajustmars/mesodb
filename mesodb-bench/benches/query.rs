// mesodb-bench/benches/query.rs

use criterion::{Criterion, criterion_group, criterion_main};
use tempfile::TempDir;

use mesodb_core::config::{Config, WalSyncMode};
use mesodb_core::db::{MesoDB, OutputFormat, QueryOptions};
use mesodb_core::schema::{SchemaMap, ValueType};
use mesodb_core::transactor::Fact;
use mesodb_core::types::Value;

fn setup_populated_db(dir: &TempDir) -> MesoDB {
    let mut schema = SchemaMap::new();
    schema.add_attribute(":user/name", ValueType::String, false);
    schema.add_attribute(":user/age", ValueType::Int64, false);
    schema.add_attribute(":order/user", ValueType::Ref, false);
    schema.add_attribute(":order/amount", ValueType::Float64, false);

    let mut config = Config::default();
    config.storage.wal_sync_mode = WalSyncMode::Background; // Fast setup
    config.storage.memtable_max_rows = 1_000_000; // Keep in RAM for raw query speed

    let db = MesoDB::open(dir.path().join("bench_query.db"), schema, config).unwrap();

    // Populate with 100,000 facts (25,000 users + 25,000 orders)
    let mut facts = Vec::with_capacity(100_000);
    for i in 1..=25_000 {
        let e_user = i as u64;
        let e_order = (i + 25_000) as u64;

        facts.push(Fact {
            e: e_user,
            ident: ":user/name".into(),
            v: Value::String(format!("User_{}", i)),
            op: true,
        });
        facts.push(Fact {
            e: e_user,
            ident: ":user/age".into(),
            v: Value::Int64(20 + (i % 50) as i64),
            op: true,
        });

        facts.push(Fact {
            e: e_order,
            ident: ":order/user".into(),
            v: Value::Ref(e_user),
            op: true,
        });
        facts.push(Fact {
            e: e_order,
            ident: ":order/amount".into(),
            v: Value::Float64(10.0 + (i as f64 % 100.0)),
            op: true,
        });
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        db.transact(facts).await.unwrap();
    });

    db
}

fn bench_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("DataFusion Query Engine (100k Facts)");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    let dir = tempfile::tempdir().unwrap();
    let db = setup_populated_db(&dir);

    // 1. Standard Relational Join (Find all order amounts for users aged 30)
    group.bench_function("Relational Join", |b| {
        let query = r#"[:find ?name ?amount
                        :where [?u :user/age 30]
                               [?u :user/name ?name]
                               [?o :order/user ?u]
                               [?o :order/amount ?amount]]"#;
        b.to_async(&rt).iter(|| async {
            db.query(query).await.unwrap();
        });
    });

    // 2. Aggregation (Sum of all orders per user age)
    group.bench_function("Aggregation (Sum/Group By)", |b| {
        let query = r#"[:find ?age (sum ?amount)
                        :where [?u :user/age ?age]
                               [?o :order/user ?u]
                               [?o :order/amount ?amount]]"#;
        b.to_async(&rt).iter(|| async {
            db.query(query).await.unwrap();
        });
    });

    // 3. Graph Traversal (Pull nested JSON for a specific user block)
    group.bench_function("Pull (Nested Graph Traversal)", |b| {
        let query = r#"[:find (pull ?u [:user/name :user/age])
                        :where [?u :user/age 30]]"#;
        let opts = QueryOptions {
            format: OutputFormat::Json,
            ..Default::default()
        };
        b.to_async(&rt).iter(|| async {
            db.query_with_options(query, opts.clone()).await.unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_queries);
criterion_main!(benches);
