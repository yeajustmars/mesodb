// mesodb-core/src/db.rs

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
    transactor: Transactor,
    ctx: SessionContext,
}

impl MesoDB {
    /// Opens or creates a new MesoDB instance at the given path.
    pub fn open<P: AsRef<Path>>(path: P, schema: SchemaMap) -> Result<Self> {
        let transactor = Transactor::new(path, schema)?;
        let ctx = SessionContext::new();

        Ok(Self { transactor, ctx })
    }

    /// Submits a batch of facts to the database.
    pub fn transact(&mut self, facts: Vec<Fact>) -> Result<TxReceipt> {
        self.transactor.transact(facts)
    }

    /// Executes a Datalog query string and returns the results as a Vec of RecordBatches.
    /// In a production scenario, you'd likely stream these or convert them to JSON.
    pub async fn query(
        &mut self,
        query_str: &str,
    ) -> Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        let ast = parse_query(query_str)?;

        // For now, we "finish" the current memtable to query it.
        // In the next version, we'll implement a rotating MemTable system.
        let batch = self.transactor.memtable.finish()?;

        let provider = DfMemTable::try_new(batch.schema(), vec![vec![batch]])?;
        self.ctx.register_table("datoms", Arc::new(provider))?;

        let planner = QueryPlanner::new(&self.ctx, &self.transactor.schema, "datoms");
        let df = planner.plan(&ast).await?;

        Ok(df.collect().await?)
    }

    /// Returns a reference to the internal schema
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

        // 1. Transact some data
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
            Fact {
                e: 2,
                ident: ":user/name".into(),
                v: Value::String("Bob".into()),
                op: true,
            },
        ])
        .unwrap();

        // 2. Query the data
        let query = r#"[:find ?n ?a :where [?e :user/name ?n] [?e :user/age ?a]]"#;
        let results = db.query(query).await.unwrap();

        // 3. Verify results
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 1); // Only Alice has both name and age

        let name_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();

        assert_eq!(name_col.value(0), "Alice");
    }
}
