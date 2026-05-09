// mesodb-core/src/db.rs
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable as DfMemTable;
use datafusion::prelude::*;
use std::path::Path;
use std::sync::Arc;

use crate::error::Result;
use crate::parser::parse_query;
use crate::planner::QueryPlanner;
use crate::schema::SchemaMap;
use crate::transactor::{Fact, Transactor, TxReceipt};

pub struct MesoDB {
    pub transactor: Transactor,
    pub ctx: SessionContext,
}

impl MesoDB {
    pub fn open<P: AsRef<Path>>(path: P, schema: SchemaMap) -> Result<Self> {
        let (flush_tx, mut flush_rx) = tokio::sync::mpsc::channel(32);
        let data_dir = path.as_ref().to_path_buf();

        // Start the background persistence worker
        tokio::spawn(async move {
            let compactor = crate::storage::BackgroundCompactor::new(data_dir);
            let mut batch_count = 0;
            while let Some(batch) = flush_rx.recv().await {
                let _ = compactor.flush_to_parquet(batch, batch_count);
                batch_count += 1;
            }
        });

        let transactor = Transactor::new(path, schema, flush_tx)?;
        let ctx = SessionContext::new();

        Ok(Self { transactor, ctx })
    }

    pub fn transact(&mut self, facts: Vec<Fact>) -> Result<TxReceipt> {
        self.transactor.transact(facts)
    }

    pub async fn query(&mut self, query_str: &str) -> Result<Vec<RecordBatch>> {
        let ast = parse_query(query_str)?;
        let batch = self.transactor.active_memtable.finish()?;

        if self.ctx.table_exist("datoms")? {
            self.ctx.deregister_table("datoms")?;
        }

        let provider = DfMemTable::try_new(batch.schema(), vec![vec![batch]])?;
        self.ctx.register_table("datoms", Arc::new(provider))?;

        let planner = QueryPlanner::new(&self.ctx, &self.transactor.schema, "datoms");
        let df = planner.plan(&ast).await?;

        Ok(df.collect().await?)
    }

    pub fn schema(&self) -> &SchemaMap {
        &self.transactor.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ValueType;
    use crate::types::Value;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_end_to_end_query() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/age", ValueType::Int64, false);

        let mut db = MesoDB::open(temp_file.path(), schema).unwrap();
        db.transact(vec![
            Fact {
                e: 1,
                ident: ":user/name".into(),
                v: Value::String("Alice".into()),
                op: true,
            },
            Fact {
                e: 1,
                ident: ":user/age".into(),
                v: Value::Int64(30),
                op: true,
            },
        ])
        .unwrap();

        let query = r#"[:find ?n ?a :where [?e :user/name ?n] [?e :user/age ?a]]"#;
        let results = db.query(query).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
    }
}
