// mesodb-core/src/planner.rs
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

use crate::ast::{FindSpec, Query, RuleDef, RuleSet, Term, WhereClause};
use crate::db::OutputFormat;
use crate::error::MesoError;
use crate::schema::SchemaMap;

pub struct QueryPlanner<'a> {
    ctx: &'a SessionContext,
    schema: &'a SchemaMap,
    table_name: &'a str,
    format: OutputFormat,
    as_of: Option<i64>,
    ruleset: Option<RuleSet>,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(
        ctx: &'a SessionContext,
        schema: &'a SchemaMap,
        table_name: &'a str,
        format: OutputFormat,
        as_of: Option<i64>,
        ruleset: Option<RuleSet>,
    ) -> Self {
        Self {
            ctx,
            schema,
            table_name,
            format,
            as_of,
            ruleset,
        }
    }

    /// Reusable engine to compile Datalog predicates into SQL JOINs
    fn build_logical_plan(
        &self,
        clauses: &[WhereClause],
    ) -> Result<(Vec<String>, Vec<String>, HashMap<String, String>), MesoError> {
        let mut from_tables = Vec::new();
        let mut where_conditions = Vec::new();
        let mut var_to_column = HashMap::new();
        let mut alias_counter = 0;

        for clause in clauses {
            match clause {
                WhereClause::DataPattern {
                    e,
                    a,
                    v,
                    tx,
                    options,
                } => {
                    let alias = format!("t{}", alias_counter);
                    from_tables.push(format!("{} AS {}", self.table_name, alias));
                    alias_counter += 1;

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
                        Term::String(s) => {
                            where_conditions.push(format!("{}.v_str = '{}'", alias, s))
                        }
                        Term::Integer(i) => {
                            where_conditions.push(format!("{}.v_int = {}", alias, i))
                        }
                        Term::Boolean(b) => {
                            where_conditions.push(format!("{}.v_bool = {}", alias, b))
                        }
                        _ => {}
                    }
                    if let Some(tx_term) = tx {
                        match tx_term {
                            Term::Variable(var_name) => {
                                let col_ref = format!("{}.t", alias);
                                if let Some(existing_col) = var_to_column.get(var_name.as_str()) {
                                    where_conditions
                                        .push(format!("{} = {}", existing_col, col_ref));
                                } else {
                                    var_to_column.insert(var_name.clone(), col_ref);
                                }
                            }
                            Term::Integer(id) => {
                                where_conditions.push(format!("{}.t = {}", alias, id))
                            }
                            _ => {}
                        }
                    }
                }
                WhereClause::RuleExpr { rule_name, args } => {
                    let alias = format!("t{}", alias_counter);
                    // FIX 1: Push recursive tables to the front as the driving left-join table!
                    from_tables.insert(0, format!("{} AS {}", rule_name, alias));
                    alias_counter += 1;

                    for (i, arg) in args.iter().enumerate() {
                        match arg {
                            Term::Variable(var_name) => {
                                let col_ref = format!("{}.col{}", alias, i);
                                if let Some(existing_col) = var_to_column.get(var_name) {
                                    where_conditions
                                        .push(format!("{} = {}", existing_col, col_ref));
                                } else {
                                    var_to_column.insert(var_name.clone(), col_ref);
                                }
                            }
                            Term::Integer(val) => {
                                where_conditions.push(format!("{}.col{} = {}", alias, i, val))
                            }
                            Term::String(val) => {
                                where_conditions.push(format!("{}.col{} = '{}'", alias, i, val))
                            }
                            _ => {}
                        }
                    }
                }
                WhereClause::Function {
                    fn_name,
                    args,
                    binding,
                } => {
                    // Convert AST terms into literal SQL arguments
                    let sql_args: Vec<String> = args
                        .iter()
                        .map(|arg| match arg {
                            Term::Variable(v) => var_to_column
                                .get(v)
                                .cloned()
                                .unwrap_or_else(|| "NULL".into()),
                            Term::Integer(i) => i.to_string(),
                            Term::Float(f) => f.to_string(),
                            Term::String(s) => format!("'{}'", s),
                            Term::Boolean(b) => b.to_string(),
                            _ => "NULL".into(),
                        })
                        .collect();

                    // Map the Datalog operator to a SQL Expression
                    let sql_expr = match fn_name.as_str() {
                        "+" | "-" | "*" | "/" => {
                            if sql_args.len() == 2 {
                                format!("({} {} {})", sql_args[0], fn_name, sql_args[1])
                            } else {
                                "NULL".into()
                            }
                        }
                        "str" => format!("CONCAT({})", sql_args.join(", ")),
                        // Comparators
                        "<" | ">" | "<=" | ">=" | "=" | "!=" => {
                            if sql_args.len() == 2 {
                                format!("{} {} {}", sql_args[0], fn_name, sql_args[1])
                            } else {
                                "FALSE".into()
                            }
                        }
                        // Fallback to natively invoking DataFusion SQL functions
                        _ => format!("{}({})", fn_name.to_uppercase(), sql_args.join(", ")),
                    };

                    if let Some(b) = binding {
                        // TRANSFORM: Bind the resulting SQL expression to the variable map
                        match b {
                            crate::ast::Binding::Scalar(var_name) => {
                                var_to_column.insert(var_name.clone(), sql_expr);
                            }
                            _ => {} // Tuples/relations not supported for simple functions yet
                        }
                    } else {
                        // PREDICATE: Push the expression directly into the WHERE conditions
                        where_conditions.push(sql_expr);
                    }
                }
                WhereClause::Or {
                    join_vars: _,
                    clauses: _,
                } => {
                    // TODO: Strike 3
                }
                WhereClause::Not {
                    join_vars: _,
                    clauses: _,
                } => {
                    // TODO: Strike 3
                }
            }
        }
        Ok((from_tables, where_conditions, var_to_column))
    }

    pub async fn plan(&self, query: &Query) -> Result<DataFrame, MesoError> {
        let mut has_pull = false;
        let mut select_vars = Vec::new();
        let mut aggregate_vars = Vec::new();

        for find_spec in &query.find {
            match find_spec {
                FindSpec::Variable(v) => select_vars.push(v.clone()),
                FindSpec::Pull(v, _) => {
                    has_pull = true;
                    select_vars.push(v.clone());
                }
                FindSpec::Aggregate(func, v) => aggregate_vars.push((func.clone(), v.clone())),
            }
        }

        if has_pull && self.format == OutputFormat::Tabular {
            return Err(MesoError::InvalidQuery(
                "Cannot return nested 'pull' data in Tabular format.".into(),
            ));
        }

        // --- CTE GENERATION FOR RULES ---
        let mut cte_blocks = Vec::new();
        if let Some(rs) = &self.ruleset {
            let mut rules_by_name: HashMap<String, Vec<&RuleDef>> = HashMap::new();
            for rule in &rs.rules {
                rules_by_name
                    .entry(rule.head.name.clone())
                    .or_default()
                    .push(rule);
            }

            for (name, mut defs) in rules_by_name {
                // FIX 2: Sort so base cases (no recursive RuleExpr) ALWAYS come first!
                defs.sort_by_key(|def| {
                    def.body
                        .iter()
                        .any(|clause| matches!(clause, WhereClause::RuleExpr { .. }))
                        as u8
                });

                let mut union_selects = Vec::new();
                let mut col_count = 0;
                for def in defs {
                    col_count = def.head.args.len();
                    let (froms, wheres, var_map) = self.build_logical_plan(&def.body)?;
                    let mut selects = Vec::new();

                    for (i, arg) in def.head.args.iter().enumerate() {
                        if let Some(col_ref) = var_map.get(arg) {
                            selects.push(format!("{} AS col{}", col_ref, i));
                        } else {
                            return Err(MesoError::PlanError(format!(
                                "Unbound variable {} in rule {}",
                                arg, name
                            )));
                        }
                    }

                    let mut sql =
                        format!("SELECT {} FROM {}", selects.join(", "), froms.join(", "));
                    if !wheres.is_empty() {
                        sql.push_str(" WHERE ");
                        sql.push_str(&wheres.join(" AND "));
                    }
                    union_selects.push(sql);
                }
                let cols: Vec<String> = (0..col_count).map(|i| format!("col{}", i)).collect();
                cte_blocks.push(format!(
                    "{}({}) AS (\n{}\n)",
                    name,
                    cols.join(", "),
                    union_selects.join("\nUNION ALL\n")
                ));
            }
        }

        // --- MAIN QUERY GENERATION ---
        let (from_tables, where_conditions, var_to_column) =
            self.build_logical_plan(&query.where_clauses)?;

        let mut select_clauses = Vec::new();
        let mut group_by_clauses = Vec::new();

        for var in &select_vars {
            if let Some(col_ref) = var_to_column.get(var) {
                let clean_var = var.replace("?", "");
                select_clauses.push(format!("{} AS \"{}\"", col_ref, clean_var));
                group_by_clauses.push(col_ref.clone());
            } else {
                return Err(MesoError::PlanError(format!(
                    "Unbound variable in find: {}",
                    var
                )));
            }
        }

        for (func, var) in &aggregate_vars {
            if let Some(col_ref) = var_to_column.get(var) {
                let clean_var = format!("{}_{}", func, var.replace("?", ""));
                let sql_func = func.to_uppercase();
                select_clauses.push(format!("{}({}) AS \"{}\"", sql_func, col_ref, clean_var));
            } else {
                return Err(MesoError::PlanError(format!(
                    "Unbound aggregate variable: {}",
                    var
                )));
            }
        }

        // PREPEND THE CTEs!
        let mut final_sql = String::new();
        if !cte_blocks.is_empty() {
            final_sql.push_str("WITH RECURSIVE ");
            final_sql.push_str(&cte_blocks.join(",\n"));
            final_sql.push_str("\n");
        }

        final_sql.push_str(&format!(
            "SELECT {} FROM {}",
            select_clauses.join(", "),
            from_tables.join(", ")
        ));

        if !where_conditions.is_empty() {
            final_sql.push_str(" WHERE ");
            final_sql.push_str(&where_conditions.join(" AND "));
        }

        if !aggregate_vars.is_empty() && !group_by_clauses.is_empty() {
            final_sql.push_str(" GROUP BY ");
            final_sql.push_str(&group_by_clauses.join(", "));
        }

        // Execute!
        let df = self
            .ctx
            .sql(&final_sql)
            .await
            .map_err(|e| MesoError::DataFusion(e))?;

        if has_pull {
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
