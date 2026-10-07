use mesodb_core::config::Config;
use mesodb_core::db::MesoDb;
use mesodb_core::schema::{SchemaMap, ValueType};
use mesodb_core::transactor::Fact;
use mesodb_core::types::Value;
use std::fs::OpenOptions;
use std::io::Write;
use tempfile::tempdir;
use tokio::time::{Duration, sleep};

// =====================================================================
// VECTOR 5: DISASTER RECOVERY CRUCIBLE
// =====================================================================

fn setup_base_schema() -> SchemaMap {
    let mut schema = SchemaMap::new();
    schema.add_attribute(":sys/health", ValueType::String, false);
    schema
}

fn setup_config() -> Config {
    let mut config = Config::default();
    // Force immediate background flushes to Parquet.
    // This ensures DataFusion can query the state upon reboot, bypassing
    // the current limitation where MesoDb doesn't rebuild RAM batches from the WAL.
    config.storage.memtable_max_rows = 1;
    config
}

#[tokio::test]
async fn test_torn_page_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("torn_page.db");

    // 1. Boot up and transact a valid genesis block
    {
        let db = MesoDb::open(&db_path, setup_base_schema(), setup_config()).unwrap();
        db.transact(vec![Fact {
            e: 1,
            ident: ":sys/health".into(),
            v: Value::String("Genesis OK".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();
        sleep(Duration::from_millis(200)).await; // Allow background thread to flush
    }

    // 2. The Disaster: Simulate a hard crash exactly mid-write.
    {
        let mut file = OpenOptions::new().append(true).open(&db_path).unwrap();
        let fake_payload = b"this_is_a_half_written_payload";
        let lied_length: u64 = 1000;
        file.write_all(&lied_length.to_ne_bytes()).unwrap();
        file.write_all(fake_payload).unwrap();
        file.sync_all().unwrap();
    }

    // 3. The Recovery
    let recovered_db = MesoDb::open(&db_path, setup_base_schema(), setup_config()).unwrap();

    // 4. Verification
    let query = r#"[:find ?health :where [1 :sys/health ?health]]"#;
    let results = recovered_db.query_native(query).await.unwrap();

    assert_eq!(
        results.len(),
        1,
        "Failed to recover the valid Genesis block."
    );
    assert_eq!(
        results[0].get("health"),
        Some(&Value::String("Genesis OK".into())),
        "Recovered data is corrupted."
    );
}

#[tokio::test]
async fn test_malicious_length_corruption() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("length_corruption.db");

    {
        let db = MesoDb::open(&db_path, setup_base_schema(), setup_config()).unwrap();
        db.transact(vec![Fact {
            e: 2,
            ident: ":sys/health".into(),
            v: Value::String("Block 1 OK".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();
        sleep(Duration::from_millis(200)).await;
    }

    // 2. The Disaster: The SSD flips bits in the length header
    {
        let mut file = OpenOptions::new().append(true).open(&db_path).unwrap();
        file.write_all(&u64::MAX.to_ne_bytes()).unwrap();
        file.sync_all().unwrap();
    }

    // 3. The Recovery
    let recovered_db = MesoDb::open(&db_path, setup_base_schema(), setup_config()).unwrap();
    let query = r#"[:find ?health :where [2 :sys/health ?health]]"#;
    let results = recovered_db.query_native(query).await.unwrap();

    assert_eq!(
        results.len(),
        1,
        "Engine panicked or failed to recover valid data due to corrupted length header."
    );
}

#[tokio::test]
async fn test_random_garbage_bytes() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("garbage.db");

    {
        let db = MesoDb::open(&db_path, setup_base_schema(), setup_config()).unwrap();
        db.transact(vec![Fact {
            e: 3,
            ident: ":sys/health".into(),
            v: Value::String("Stable".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();
        sleep(Duration::from_millis(200)).await;
    }

    // 2. The Disaster: Pure garbage bytes
    {
        let mut file = OpenOptions::new().append(true).open(&db_path).unwrap();
        file.write_all(b"!!GARBAGE_BYTES_THAT_DO_NOT_MATCH_ANY_STRUCTURE!!")
            .unwrap();
        file.sync_all().unwrap();
    }

    // 3. The Recovery
    let recovered_db = MesoDb::open(&db_path, setup_base_schema(), setup_config()).unwrap();
    let query = r#"[:find ?health :where [3 :sys/health ?health]]"#;
    let results = recovered_db.query_native(query).await.unwrap();

    assert_eq!(
        results.len(),
        1,
        "Engine failed to parse around random garbage bytes."
    );
}
