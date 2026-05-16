// mesodb-core/src/planner.rs
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

use crate::ast::{FindSpec, Query, Term, WhereClause};
use crate::db::OutputFormat;
use crate::error::MesoError;
use crate::schema::SchemaMap;

pub struct QueryPlanner<'a> {
    ctx: &'a SessionContext,
    schema: &'a SchemaMap,
    table_name: &'a str,
    format: OutputFormat,
    as_of: Option<i64>,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(
        ctx: &'a SessionContext,
        schema: &'a SchemaMap,
        table_name: &'a str,
        format: OutputFormat,
        as_of: Option<i64>,
    ) -> Self {
        Self {
            ctx,
            schema,
            table_name,
            format,
            as_of,
        }
    }

    pub async fn plan(&self, query: &Query) -> Result<DataFrame, MesoError> {
        // 1. Strict Formatting Validation
        let mut has_pull = false;
        let mut select_vars = Vec::new();
        let mut aggregate_vars = Vec::new(); // Track our aggregations

        for find_spec in &query.find {
            match find_spec {
                FindSpec::Variable(v) => select_vars.push(v.clone()),
                FindSpec::Pull(v, _) => {
                    has_pull = true;
                    select_vars.push(v.clone());
                }
                FindSpec::Aggregate(func, v) => {
                    aggregate_vars.push((func.clone(), v.clone()));
                }
            }
        }

        if has_pull && self.format == OutputFormat::Tabular {
            return Err(MesoError::InvalidQuery(
                "Cannot return nested 'pull' data in Tabular format. Please use 'edn' or 'json' output formats.".into(),
            ));
        }

        // 2. Build the Logical Joins (Implicit Join Style)
        let mut from_tables = Vec::new();
        let mut where_conditions = Vec::new();
        let mut var_to_column = HashMap::new();
        let mut alias_counter = 0;

        for clause in &query.where_clauses {
            if let WhereClause::DataPattern {
                e,
                a,
                v,
                tx: _,
                options,
            } = clause
            {
                let alias = format!("t{}", alias_counter);
                from_tables.push(format!("{} AS {}", self.table_name, alias));
                alias_counter += 1;

                // Process Temporal Options (from our options map)
                let mut has_inline_time = false;
                if let Some(opts) = options {
                    if let Some(Term::Integer(t)) = opts.get(":at") {
                        has_inline_time = true;
                        where_conditions.push(format!("CAST({}.valid_from AS BIGINT) <= {} AND CAST({}.valid_to AS BIGINT) > {}", alias, t, alias, t));
                    } else if let Some(Term::Integer(t)) = opts.get(":since") {
                        has_inline_time = true;
                        where_conditions.push(format!("CAST({}.t AS BIGINT) >= {}", alias, t));
                    }
                }

                if !has_inline_time && self.format != OutputFormat::Edn {
                    where_conditions.push(format!("{}.next_from IS NULL", alias));
                }

                // Process E
                match e {
                    Term::Variable(var_name) => {
                        let col_ref = format!("{}.e", alias);
                        if let Some(existing_col) = var_to_column.get(var_name) {
                            where_conditions.push(format!("{} = {}", existing_col, col_ref));
                        } else {
                            var_to_column.insert(var_name.clone(), col_ref);
                        }
                    }
                    Term::Integer(id) => where_conditions.push(format!("{}.e = {}", alias, id)),
                    _ => {}
                }

                // Process A
                let mut current_attr = None;
                match a {
                    Term::Keyword(kw) => {
                        let attr_id = self.schema.get_id(kw).ok_or_else(|| {
                            MesoError::PlanError(format!("Unknown attribute: {}", kw))
                        })?;
                        current_attr = self.schema.get_by_id(attr_id);
                        where_conditions.push(format!("{}.a = {}", alias, attr_id));
                    }
                    Term::Variable(var_name) => {
                        let col_ref = format!("{}.a", alias);
                        if let Some(existing_col) = var_to_column.get(var_name) {
                            where_conditions.push(format!("{} = {}", existing_col, col_ref));
                        } else {
                            var_to_column.insert(var_name.clone(), col_ref);
                        }
                    }
                    _ => {}
                }

                // Process V
                match v {
                    Term::Variable(var_name) => {
                        let col_type = if let Some(attr) = current_attr {
                            match attr.value_type {
                                crate::schema::ValueType::Boolean => "v_bool",
                                crate::schema::ValueType::Int64 => "v_int",
                                crate::schema::ValueType::Float64 => "v_float",
                                crate::schema::ValueType::String => "v_str",
                                crate::schema::ValueType::Ref => "v_ref",
                                crate::schema::ValueType::Timestamp => "v_time",
                                crate::schema::ValueType::Uuid => "v_uuid",
                            }
                        } else {
                            "v_str"
                        };

                        let col_ref = format!("{}.{}", alias, col_type);
                        if let Some(existing_col) = var_to_column.get(var_name) {
                            where_conditions.push(format!("{} = {}", existing_col, col_ref));
                        } else {
                            var_to_column.insert(var_name.clone(), col_ref);
                        }
                    }
                    Term::String(s) => where_conditions.push(format!("{}.v_str = '{}'", alias, s)),
                    Term::Integer(i) => where_conditions.push(format!("{}.v_int = {}", alias, i)),
                    Term::Boolean(b) => where_conditions.push(format!("{}.v_bool = {}", alias, b)),
                    _ => {}
                }
            }
        }

        // 3. Projection Phase (SELECT & GROUP BY)
        let mut select_clauses = Vec::new();
        let mut group_by_clauses = Vec::new();

        // Standard variables
        for var in &select_vars {
            if let Some(col_ref) = var_to_column.get(var) {
                let clean_var = var.replace("?", "");
                select_clauses.push(format!("{} AS \"{}\"", col_ref, clean_var));
                group_by_clauses.push(col_ref.clone()); // We must group by all standard variables
            } else {
                return Err(MesoError::PlanError(format!(
                    "Unbound variable in find: {}",
                    var
                )));
            }
        }

        // Aggregation variables
        for (func, var) in &aggregate_vars {
            if let Some(col_ref) = var_to_column.get(var) {
                let clean_var = format!("{}_{}", func, var.replace("?", ""));
                // Map datalog func (count, sum, max, min) directly to SQL func
                let sql_func = func.to_uppercase();
                select_clauses.push(format!("{}({}) AS \"{}\"", sql_func, col_ref, clean_var));
            } else {
                return Err(MesoError::PlanError(format!(
                    "Unbound aggregate variable: {}",
                    var
                )));
            }
        }

        let mut final_sql = format!(
            "SELECT {} FROM {}",
            select_clauses.join(", "),
            from_tables.join(", ")
        );

        if !where_conditions.is_empty() {
            final_sql.push_str(" WHERE ");
            final_sql.push_str(&where_conditions.join(" AND "));
        }

        // Apply GROUP BY if there are mixed standard and aggregate vars
        if !aggregate_vars.is_empty() && !group_by_clauses.is_empty() {
            final_sql.push_str(" GROUP BY ");
            final_sql.push_str(&group_by_clauses.join(", "));
        }

        // Execute the relational base query
        let df = self
            .ctx
            .sql(&final_sql)
            .await
            .map_err(MesoError::DataFusion)?;

        if has_pull {
            // PHASE 2: Graph Traversal Interception
            let batches = df.clone().collect().await.map_err(MesoError::DataFusion)?;
            if batches.is_empty() {
                return Ok(df);
            }

            let mut final_batches = Vec::new();

            for batch in batches {
                let mut new_columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
                let mut new_fields = Vec::new();

                for (i, find_spec) in query.find.iter().enumerate() {
                    match find_spec {
                        FindSpec::Variable(v) => {
                            let clean_var = v.replace("?", "");
                            new_columns.push(batch.column(i).clone());
                            new_fields.push(arrow::datatypes::Field::new(
                                clean_var,
                                batch.column(i).data_type().clone(),
                                true,
                            ));
                        }
                        FindSpec::Pull(v, pattern) => {
                            let clean_var = v.replace("?", "");
                            let e_col = batch
                                .column(i)
                                .as_any()
                                .downcast_ref::<arrow::array::UInt64Array>()
                                .unwrap();
                            let e_ids: Vec<u64> =
                                (0..e_col.len()).map(|idx| e_col.value(idx)).collect();

                            let pull_engine = crate::pull::PullEngine::new(
                                self.ctx,
                                self.schema,
                                &self.format,
                                self.as_of,
                            );
                            let string_arr = pull_engine.execute_pull(&e_ids, pattern).await?;

                            new_columns.push(Arc::new(string_arr));
                            new_fields.push(arrow::datatypes::Field::new(
                                clean_var,
                                arrow::datatypes::DataType::Utf8,
                                true,
                            ));
                        }
                        FindSpec::Aggregate(func, v) => {
                            let clean_var = format!("{}_{}", func, v.replace("?", ""));
                            new_columns.push(batch.column(i).clone());
                            new_fields.push(arrow::datatypes::Field::new(
                                clean_var,
                                batch.column(i).data_type().clone(),
                                true,
                            ));
                        }
                    }
                }

                let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
                let new_batch =
                    RecordBatch::try_new(new_schema, new_columns).map_err(MesoError::Arrow)?;
                final_batches.push(new_batch);
            }

            let mem_table = datafusion::datasource::memory::MemTable::try_new(
                final_batches[0].schema(),
                vec![final_batches],
            )
            .unwrap();
            let temp_ctx = SessionContext::new();
            temp_ctx.register_table("pull_results", Arc::new(mem_table))?;
            temp_ctx
                .sql("SELECT * FROM pull_results")
                .await
                .map_err(MesoError::DataFusion)
        } else {
            Ok(df)
        }
    }
}
