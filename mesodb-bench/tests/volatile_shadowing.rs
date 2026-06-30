use tempfile::tempdir;
use tokio::time::{Duration, sleep};

use mesodb_core::config::{Config, WalSyncMode};
use mesodb_core::db::MesoDB;
use mesodb_core::schema::{SchemaMap, ValueType};
use mesodb_core::transactor::Fact;
use mesodb_core::types::Value;

// =====================================================================
// VECTOR 2: VOLATILE SHADOWING SUITE
// =====================================================================

/// Forces the transactor to exceed the hardcoded 50,000 row threshold in `db.rs`
/// to guarantee the background compactor moves the batch to Parquet.
async fn force_disk_flush(db: &MesoDB, start_e: u64) {
    let mut dummy_facts = Vec::with_capacity(50_001);
    for i in 0..50_001 {
        dummy_facts.push(Fact {
            e: start_e + i,
            ident: ":sys/dummy".into(),
            v: Value::Int64(i as i64),
            op: true,
            cas_old_v: None,
            valid_time: None,
        });
    }
    db.transact(dummy_facts).await.unwrap();
    // Yield to the Tokio runtime so the background worker can pick up the channel message
    sleep(Duration::from_millis(500)).await;
}

fn setup_shadow_db(dir: &tempfile::TempDir) -> MesoDB {
    let mut schema = SchemaMap::new();
    schema.add_attribute(":user/name", ValueType::String, false);
    schema.add_attribute(":user/age", ValueType::Int64, false);
    schema.add_attribute(":user/status", ValueType::String, false);
    schema.add_attribute(":sys/dummy", ValueType::Int64, false); // For the flush payload

    let mut config = Config::default();
    config.storage.wal_sync_mode = WalSyncMode::Background;

    MesoDB::open(dir.path().join("shadow.db"), schema, config).unwrap()
}

#[tokio::test]
async fn test_total_eclipse_masking() {
    let dir = tempdir().unwrap();
    let db = setup_shadow_db(&dir);

    let target_e = 1;

    // 1. Write the initial state to Disk
    db.transact(vec![
        Fact {
            e: target_e,
            ident: ":user/name".into(),
            v: Value::String("Alice".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        },
        Fact {
            e: target_e,
            ident: ":user/age".into(),
            v: Value::Int64(30),
            op: true,
            cas_old_v: None,
            valid_time: None,
        },
    ])
    .await
    .unwrap();

    force_disk_flush(&db, 1000).await;

    // 2. Completely rewrite the entity in RAM (The Eclipse)
    db.transact(vec![
        Fact {
            e: target_e,
            ident: ":user/name".into(),
            v: Value::String("Alice-Revised".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        },
        Fact {
            e: target_e,
            ident: ":user/age".into(),
            v: Value::Int64(31),
            op: true,
            cas_old_v: None,
            valid_time: None,
        },
    ])
    .await
    .unwrap();

    // 3. Query the engine. The NOT EXISTS mask MUST completely block the Parquet rows.
    let query = r#"[:find ?name ?age :where [1 :user/name ?name] [1 :user/age ?age]]"#;
    let results = db.query_native(query).await.unwrap();

    assert_eq!(
        results.len(),
        1,
        "Total eclipse failed: DataFusion leaked duplicate historical rows through the anti-join."
    );
    assert_eq!(
        results[0].get("name"),
        Some(&Value::String("Alice-Revised".into()))
    );
    assert_eq!(results[0].get("age"), Some(&Value::Int64(31)));
}

#[tokio::test]
async fn test_partial_eclipse_merge() {
    let dir = tempdir().unwrap();
    let db = setup_shadow_db(&dir);

    let target_e = 2;

    // 1. Write a wide entity to Disk
    db.transact(vec![
        Fact {
            e: target_e,
            ident: ":user/name".into(),
            v: Value::String("Bob".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        },
        Fact {
            e: target_e,
            ident: ":user/age".into(),
            v: Value::Int64(40),
            op: true,
            cas_old_v: None,
            valid_time: None,
        },
        Fact {
            e: target_e,
            ident: ":user/status".into(),
            v: Value::String("Active".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        },
    ])
    .await
    .unwrap();

    force_disk_flush(&db, 100_000).await;

    // 2. Mutate exactly ONE attribute in RAM (Partial Eclipse)
    db.transact(vec![Fact {
        e: target_e,
        ident: ":user/age".into(),
        v: Value::Int64(41),
        op: true,
        cas_old_v: None,
        valid_time: None,
    }])
    .await
    .unwrap();

    // 3. Query all attributes. The engine must seamlessly merge the RAM age with the Disk name & status.
    let query = r#"[:find ?name ?age ?status :where [2 :user/name ?name] [2 :user/age ?age] [2 :user/status ?status]]"#;
    let results = db.query_native(query).await.unwrap();

    assert_eq!(
        results.len(),
        1,
        "Partial eclipse failed: The stream router could not merge RAM and Disk states."
    );
    assert_eq!(
        results[0].get("name"),
        Some(&Value::String("Bob".into())),
        "Failed to pull un-eclipsed attribute from Disk."
    );
    assert_eq!(
        results[0].get("status"),
        Some(&Value::String("Active".into())),
        "Failed to pull un-eclipsed attribute from Disk."
    );
    assert_eq!(
        results[0].get("age"),
        Some(&Value::Int64(41)),
        "Failed to pull eclipsed attribute from RAM."
    );
}
