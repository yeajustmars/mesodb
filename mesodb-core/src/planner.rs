// mesodb-core/src/planner.rs

use datafusion::logical_expr::{Expr, JoinType, col};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use std::collections::{BTreeMap, HashSet};

use crate::ast::{FindSpec, Query, Term, WhereClause};
use crate::error::MesoError;
use crate::schema::{SchemaMap, ValueType};
use crate::types::Result;

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

    fn term_to_micros(&self, term: &Term) -> Result<i64> {
        match term {
            Term::Integer(i) => Ok(*i),
            Term::String(s) => s
                .parse::<i64>()
                .map_err(|_| MesoError::PlanError(format!("Invalid date string: {}", s))),
            _ => Err(MesoError::PlanError(
                "Time must be Integer or String".into(),
            )),
        }
    }

    fn ts_lit(val: i64) -> Expr {
        // DataFusion 50+ requires (ScalarValue, Option<FieldMetadata>)
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(val), None), None)
    }

    pub async fn plan(&self, query: &Query) -> Result<DataFrame> {
        let mut current_df: Option<DataFrame> = None;
        let mut known_vars: HashSet<String> = HashSet::new();

        for clause in &query.where_clauses {
            match clause {
                WhereClause::DataPattern {
                    e,
                    a,
                    v,
                    tx,
                    options,
                } => {
                    let mut next_df = self.plan_data_pattern(e, a, v, tx, options).await?;

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
                                // Add the extra None here
                                Some(Expr::Literal(ScalarValue::Boolean(Some(true)), None)),
                            )?);
                        } else {
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

    async fn plan_data_pattern(
        &self,
        e: &Term,
        a: &Term,
        v: &Term,
        tx: &Option<Term>,
        options: &Option<BTreeMap<String, Term>>,
    ) -> Result<DataFrame> {
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

        if let Some(Term::Integer(tx_id)) = tx {
            filters.push(col("t").eq(lit(*tx_id as u64)));
        }

        if let Some(opts) = options {
            for (key, val) in opts {
                match key.as_str() {
                    ":at" => {
                        let t = self.term_to_micros(val)?;
                        filters.push(col("valid_from").lt_eq(Self::ts_lit(t)));
                        filters.push(col("valid_to").gt(Self::ts_lit(t)));
                    }
                    ":since" => {
                        let t = self.term_to_micros(val)?;
                        filters.push(col("valid_from").gt_eq(Self::ts_lit(t)));
                    }
                    ":before" => {
                        let t = self.term_to_micros(val)?;
                        filters.push(col("valid_to").lt_eq(Self::ts_lit(t)));
                    }
                    ":between" => {
                        if let Term::Vector(vec) = val {
                            if vec.len() == 2 {
                                let start = self.term_to_micros(&vec[0])?;
                                let end = self.term_to_micros(&vec[1])?;
                                filters.push(col("valid_from").lt(Self::ts_lit(end)));
                                filters.push(col("valid_to").gt(Self::ts_lit(start)));
                            }
                        }
                    }
                    _ => return Err(MesoError::PlanError(format!("Unknown option: {}", key))),
                }
            }
        } else {
            // Compare Timestamp to Timestamp literal
            filters.push(col("valid_to").eq(Self::ts_lit(i64::MAX)));
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
