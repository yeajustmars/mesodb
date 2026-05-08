// mesodb-core/src/planner.rs

use std::collections::HashSet;

use datafusion::logical_expr::{JoinType, col, lit};
use datafusion::prelude::*;

use crate::ast::{FindSpec, Query, Term, WhereClause};
use crate::error::{MesoError, Result};
use crate::schema::{SchemaMap, ValueType};

/// Translates a Datalog AST Query into an executable DataFusion DataFrame.
pub struct QueryPlanner<'a> {
    ctx: &'a SessionContext,
    schema: &'a SchemaMap,
    table_name: &'a str,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(ctx: &'a SessionContext, schema: &'a SchemaMap, table_name: &'a str) -> Self {
        Self {
            ctx,
            schema,
            table_name,
        }
    }

    /// Compiles the AST into a single execution plan.
    pub async fn plan(&self, query: &Query) -> Result<DataFrame> {
        let mut current_df: Option<DataFrame> = None;
        let mut known_vars: HashSet<String> = HashSet::new();

        // 1. Process WHERE clauses to build the base relations and joins
        for clause in &query.where_clauses {
            match clause {
                WhereClause::DataPattern { e, a, v, .. } => {
                    let mut next_df = self.plan_data_pattern(e, a, v).await?;

                    if let Some(left_df) = current_df {
                        let new_vars = self.extract_vars(e, v);
                        let join_cols: Vec<String> =
                            new_vars.intersection(&known_vars).cloned().collect();

                        if join_cols.is_empty() {
                            current_df = Some(left_df.join(
                                next_df,
                                JoinType::Inner,
                                &[],
                                &[],
                                Some(lit(true)),
                            )?);
                        } else {
                            // 1. Alias the right-hand columns to avoid name collisions during join
                            let mut right_keys = Vec::new();
                            let mut select_exprs = Vec::new();

                            for field in next_df.schema().fields() {
                                let name = field.name();
                                if join_cols.contains(name) {
                                    let alias = format!("{}_right", name);
                                    select_exprs.push(col(name).alias(&alias));
                                    right_keys.push(alias);
                                } else {
                                    select_exprs.push(col(name));
                                }
                            }
                            next_df = next_df.select(select_exprs)?;

                            // 2. Perform the join using the aliases for the right side
                            let left_keys: Vec<&str> =
                                join_cols.iter().map(|s| s.as_str()).collect();
                            let right_keys_str: Vec<&str> =
                                right_keys.iter().map(|s| s.as_str()).collect();

                            let joined = left_df.join(
                                next_df,
                                JoinType::Inner,
                                &left_keys,
                                &right_keys_str,
                                None,
                            )?;

                            // 3. Drop the aliased columns
                            let drop_exprs: Vec<Expr> = joined
                                .schema()
                                .fields()
                                .iter()
                                .filter(|f| !right_keys.contains(f.name()))
                                .map(|f| col(f.name()))
                                .collect();

                            current_df = Some(joined.select(drop_exprs)?);
                        }
                        known_vars.extend(new_vars);
                    } else {
                        current_df = Some(next_df);
                        known_vars.extend(self.extract_vars(e, v));
                    }
                }
                _ => {
                    return Err(MesoError::PlanError(
                        "Only DataPatterns are supported".into(),
                    ));
                }
            }
        }

        let final_df = current_df.ok_or_else(|| {
            MesoError::PlanError("Query must have at least one WHERE clause".into())
        })?;

        // 2. Process FIND clause
        let mut select_exprs = Vec::new();
        for find_spec in &query.find {
            match find_spec {
                FindSpec::Variable(var_name) => select_exprs.push(col(var_name)),
                _ => {
                    return Err(MesoError::PlanError(
                        "Only variable projections supported".into(),
                    ));
                }
            }
        }

        Ok(final_df.select(select_exprs)?)
    }

    async fn plan_data_pattern(&self, e: &Term, a: &Term, v: &Term) -> Result<DataFrame> {
        let mut df = self.ctx.table(self.table_name).await?;
        let mut filters = Vec::new();
        let mut projections = Vec::new();

        let attr_id = match a {
            Term::Keyword(kw) => {
                let attr = self
                    .schema
                    .get_by_ident(kw)
                    .ok_or_else(|| MesoError::UndefinedAttribute(kw.clone()))?;
                attr.id
            }
            _ => return Err(MesoError::PlanError("Attribute must be a Keyword".into())),
        };
        filters.push(col("a").eq(lit(attr_id)));

        match e {
            Term::Variable(var) => projections.push(col("e").alias(var)),
            Term::Integer(id) => filters.push(col("e").eq(lit(*id as u64))),
            _ => {
                return Err(MesoError::PlanError(
                    "Entity must be Variable or Integer".into(),
                ));
            }
        }

        let val_col = self.get_column_for_attr(attr_id)?;
        match v {
            Term::Variable(var) => projections.push(col(val_col).alias(var)),
            Term::String(s) => filters.push(col(val_col).eq(lit(s.clone()))),
            Term::Integer(i) => filters.push(col(val_col).eq(lit(*i))),
            Term::Float(f) => filters.push(col(val_col).eq(lit(*f))),
            Term::Boolean(b) => filters.push(col(val_col).eq(lit(*b))),
            Term::Blank => {}
            _ => return Err(MesoError::PlanError("Unsupported value term".into())),
        }

        for filter in filters {
            df = df.filter(filter)?;
        }

        if projections.is_empty() {
            projections.push(lit(1).alias("_dummy_match"));
        }

        Ok(df.select(projections)?)
    }

    fn get_column_for_attr(&self, attr_id: u32) -> Result<&'static str> {
        let attr = self.schema.get_by_id(attr_id).unwrap();
        match attr.value_type {
            ValueType::Boolean => Ok("v_bool"),
            ValueType::Int64 => Ok("v_int"),
            ValueType::Float64 => Ok("v_float"),
            ValueType::String => Ok("v_str"),
            ValueType::Ref => Ok("v_ref"),
            ValueType::Timestamp => Ok("v_time"),
            ValueType::Uuid => Ok("v_uuid"),
        }
    }

    fn extract_vars(&self, e: &Term, v: &Term) -> HashSet<String> {
        let mut vars = HashSet::new();
        if let Term::Variable(var) = e {
            vars.insert(var.clone());
        }
        if let Term::Variable(var) = v {
            vars.insert(var.clone());
        }
        vars
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::MemTable;
    use crate::parser::parse_query;
    use datafusion::datasource::memory::MemTable as DfMemTable;
    use std::sync::Arc;

    async fn setup_df_context(_schema: &SchemaMap) -> SessionContext {
        let mut memtable = MemTable::new(1);
        let arrow_batch = memtable.finish().unwrap();

        let ctx = SessionContext::new();
        let provider = DfMemTable::try_new(arrow_batch.schema(), vec![vec![arrow_batch]]).unwrap();
        ctx.register_table("datoms", Arc::new(provider)).unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_planner_implicit_inner_join() {
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/age", ValueType::Int64, false);

        let ctx = setup_df_context(&schema).await;
        let planner = QueryPlanner::new(&ctx, &schema, "datoms");

        let q_str = r#"
            [:find ?age
             :where [?e :user/name "Alice"]
                    [?e :user/age ?age]]
        "#;
        let query_ast = parse_query(q_str).unwrap();

        let df = planner
            .plan(&query_ast)
            .await
            .expect("Failed to plan query");
        let plan_str = format!("{:?}", df.logical_plan());

        assert!(plan_str.contains("Inner"));
        assert!(plan_str.contains("?e"));
    }

    // --- ADVANCED PLANNER TESTS ---

    #[tokio::test]
    async fn test_planner_self_join() {
        // Find entities that share the same name (a classic self-join)
        // [:find ?e1 ?e2 :where [?e1 :user/name ?name] [?e2 :user/name ?name]]
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);

        let ctx = setup_df_context(&schema).await;
        let planner = QueryPlanner::new(&ctx, &schema, "datoms");

        let q_str = r#"[:find ?e1 ?e2 :where [?e1 :user/name ?name] [?e2 :user/name ?name]]"#;
        let query_ast = parse_query(q_str).unwrap();

        let df = planner
            .plan(&query_ast)
            .await
            .expect("Self-join planning failed");
        let schema = df.schema();

        // Ensure we have all three variables in the plan, but only projected 2
        assert_eq!(df.logical_plan().schema().fields().len(), 2);
        assert!(schema.field_with_name(None, "?e1").is_ok());
        assert!(schema.field_with_name(None, "?e2").is_ok());
    }

    #[tokio::test]
    async fn test_planner_three_hop_chain() {
        // Testing a chain of joins: e -> a -> b -> name
        // [:find ?name :where [?e :user/address ?a] [?a :address/city ?c] [?c :city/name ?name]]
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/address", ValueType::Ref, false);
        schema.add_attribute(":address/city", ValueType::Ref, false);
        schema.add_attribute(":city/name", ValueType::String, false);

        let ctx = setup_df_context(&schema).await;
        let planner = QueryPlanner::new(&ctx, &schema, "datoms");

        let q_str = r#"
            [:find ?name
             :where [?e :user/address ?a]
                    [?a :address/city ?c]
                    [?c :city/name ?name]]
        "#;
        let query_ast = parse_query(q_str).unwrap();
        let df = planner
            .plan(&query_ast)
            .await
            .expect("Chain join planning failed");

        // The final result should only contain the requested variable
        assert_eq!(df.schema().fields().len(), 1);
        assert!(df.schema().field_with_name(None, "?name").is_ok());
    }

    #[tokio::test]
    async fn test_planner_cross_join_logic() {
        // Independent variables should trigger a cross-join (Cartesian Product)
        // [:find ?n ?c :where [?e1 :user/name ?n] [?e2 :city/name ?c]]
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":city/name", ValueType::String, false);

        let ctx = setup_df_context(&schema).await;
        let planner = QueryPlanner::new(&ctx, &schema, "datoms");

        let q_str = r#"[:find ?n ?c :where [?e1 :user/name ?n] [?e2 :city/name ?c]]"#;
        let query_ast = parse_query(q_str).unwrap();

        let df = planner
            .plan(&query_ast)
            .await
            .expect("Cross-join planning failed");

        // In the logical plan, this should manifest as a Join with no keys and a literal true filter
        let plan_str = format!("{:?}", df.logical_plan());
        assert!(plan_str.contains("Inner") || plan_str.contains("Cross"));
    }

    #[tokio::test]
    async fn test_planner_mixed_literals_and_vars() {
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/active", ValueType::Boolean, false);
        schema.add_attribute(":user/role", ValueType::String, false);

        let ctx = setup_df_context(&schema).await;
        let planner = QueryPlanner::new(&ctx, &schema, "datoms");

        let q_str = r#"[:find ?e :where [?e :user/active true] [?e :user/role "admin"]] "#;
        let query_ast = parse_query(q_str).unwrap();

        let df = planner
            .plan(&query_ast)
            .await
            .expect("Literal filtering failed");
        assert!(df.schema().field_with_name(None, "?e").is_ok());
    }

    #[tokio::test]
    async fn test_planner_entity_is_literal() {
        // Querying attributes for a specific known Entity ID
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);

        let ctx = setup_df_context(&schema).await;
        let planner = QueryPlanner::new(&ctx, &schema, "datoms");

        let q_str = r#"[:find ?n :where [12345 :user/name ?n]]"#;
        let query_ast = parse_query(q_str).unwrap();

        let df = planner
            .plan(&query_ast)
            .await
            .expect("Entity literal planning failed");
        let plan_str = format!("{:?}", df.logical_plan());
        assert!(plan_str.contains("12345"));
    }

    #[tokio::test]
    async fn test_planner_triangular_join() {
        // A shares a variable with B, B shares with C, C shares with A.
        // [:find ?e1 :where [?e1 :friend ?e2] [?e2 :friend ?e3] [?e3 :friend ?e1]]
        let mut schema = SchemaMap::new();
        schema.add_attribute(":friend", ValueType::Ref, false);

        let ctx = setup_df_context(&schema).await;
        let planner = QueryPlanner::new(&ctx, &schema, "datoms");

        let q_str = r#"[:find ?e1 :where [?e1 :friend ?e2] [?e2 :friend ?e3] [?e3 :friend ?e1]]"#;
        let query_ast = parse_query(q_str).unwrap();

        let result = planner.plan(&query_ast).await;
        assert!(
            result.is_ok(),
            "Triangular join should not cause schema collisions"
        );
    }

    #[tokio::test]
    async fn test_planner_blank_node_handling() {
        // Blank nodes "_" should be ignored by the planner (existential check)
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/email", ValueType::String, false);

        let ctx = setup_df_context(&schema).await;
        let planner = QueryPlanner::new(&ctx, &schema, "datoms");

        let q_str = r#"[:find ?e :where [?e :user/email _]]"#;
        let query_ast = parse_query(q_str).unwrap();

        let df = planner
            .plan(&query_ast)
            .await
            .expect("Blank node planning failed");
        // Schema should only have ?e, not any column for the email value
        assert_eq!(df.schema().fields().len(), 1);
    }

    #[tokio::test]
    async fn test_planner_multiple_projections() {
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/email", ValueType::String, false);

        let ctx = setup_df_context(&schema).await;
        let planner = QueryPlanner::new(&ctx, &schema, "datoms");

        let q_str = r#"[:find ?n ?m :where [?e :user/name ?n] [?e :user/email ?m]]"#;
        let query_ast = parse_query(q_str).unwrap();

        let df = planner
            .plan(&query_ast)
            .await
            .expect("Multi-projection failed");
        assert_eq!(df.schema().fields().len(), 2);
        assert!(df.schema().field_with_name(None, "?n").is_ok());
        assert!(df.schema().field_with_name(None, "?m").is_ok());
    }

    #[tokio::test]
    async fn test_planner_rejects_unsupported_find() {
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);

        let ctx = setup_df_context(&schema).await;
        let planner = QueryPlanner::new(&ctx, &schema, "datoms");

        // (count ?e) is parsed by the parser but not yet implemented in the planner
        let q_str = r#"[:find (count ?e) :where [?e :user/name ?n]]"#;
        let query_ast = parse_query(q_str).unwrap();

        let result = planner.plan(&query_ast).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_planner_undefined_attribute_fails() {
        let schema = SchemaMap::new(); // Empty schema
        let ctx = setup_df_context(&schema).await;
        let planner = QueryPlanner::new(&ctx, &schema, "datoms");

        let q_str = r#"[:find ?e :where [?e :ghost/attribute "boo"]] "#;
        let query_ast = parse_query(q_str).unwrap();

        let result = planner.plan(&query_ast).await;
        assert!(result.is_err());
        assert!(format!("{:?}", result.unwrap_err()).contains("UndefinedAttribute"));
    }
}
