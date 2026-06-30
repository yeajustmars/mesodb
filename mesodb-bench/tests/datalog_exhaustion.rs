use std::sync::Arc;
use tempfile::tempdir;

use mesodb_core::config::{Config, WalSyncMode};
use mesodb_core::db::{MesoDB, QueryOptions};
use mesodb_core::schema::{SchemaMap, ValueType};
use mesodb_core::transactor::Fact;
use mesodb_core::types::Value;

// =====================================================================
// VECTOR 3: DATALOG EXHAUSTION SUITE
// =====================================================================

fn setup_exhaustion_db(dir: &tempfile::TempDir) -> Arc<MesoDB> {
    let mut schema = SchemaMap::new();
    schema.add_attribute(":node/parent", ValueType::Ref, false);
    schema.add_attribute(":node/name", ValueType::String, false);

    let mut config = Config::default();
    config.storage.wal_sync_mode = WalSyncMode::Background;

    // THE CRUCIBLE: Set an aggressively low threshold to force constant Disk flushing
    // and WorldView pointer swaps during query evaluation.
    config.storage.memtable_max_rows = 50;

    Arc::new(MesoDB::open(dir.path().join("exhaustion.db"), schema, config).unwrap())
}

#[tokio::test]
async fn test_mid_flight_compaction_race() {
    let dir = tempdir().unwrap();
    let db = setup_exhaustion_db(&dir);

    // 1. Build a deep graph structure (50 nodes deep)
    let mut facts = Vec::new();
    for i in 1..=50 {
        facts.push(Fact {
            e: i,
            ident: ":node/name".into(),
            v: Value::String(format!("Node_{}", i)),
            op: true,
            cas_old_v: None,
            valid_time: None,
        });
        if i > 1 {
            // Link each node to the previous one
            facts.push(Fact {
                e: i,
                ident: ":node/parent".into(),
                v: Value::Ref(i - 1),
                op: true,
                cas_old_v: None,
                valid_time: None,
            });
        }
    }
    db.transact(facts).await.unwrap();

    let rules = r#"
    [
        [(ancestor ?child ?parent)
         [?child :node/parent ?parent]]

        [(ancestor ?child ?ancestor)
         [?child :node/parent ?parent]
         (ancestor ?parent ?ancestor)]
    ]
    "#;

    // "Find ALL ancestors (recursive) for Node 50"
    let query = r#"[:find ?ancestor_name :where (ancestor 50 ?a) [?a :node/name ?ancestor_name]]"#;
    let opts = QueryOptions {
        rules: Some(rules.into()),
        ..Default::default()
    };

    // 2. Spawn a background task to aggressively spam transactions.
    // This forces continuous Option A background compactions and lock-free
    // WorldView pointer swaps exactly while the recursive query is processing.
    let bg_db = db.clone();
    let bg_task = tokio::spawn(async move {
        for i in 1000..2000 {
            let spam_facts = vec![Fact {
                e: i,
                ident: ":node/name".into(),
                v: Value::String(format!("Spam_{}", i)),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }];
            bg_db.transact(spam_facts).await.unwrap();
            // Yield to the Tokio runtime to let the background compactor thread fight for locks
            tokio::task::yield_now().await;
        }
    });

    // 3. Execute the recursive query repeatedly during the chaos
    for _ in 0..10 {
        let results = db
            .query_native_with_options(query, opts.clone())
            .await
            .unwrap();
        // A depth of 50 means Node 50 has exactly 49 ancestors.
        assert_eq!(
            results.len(),
            49,
            "Recursive rule resolution failed or dropped rows during mid-flight state migration."
        );
    }

    // Clean up
    bg_task.abort();
}
