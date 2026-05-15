// mesodb-core/src/parser.rs
use pest::Parser;
use pest_derive::Parser;
use std::collections::BTreeMap;

use crate::ast::*;
use crate::error::MesoError;

#[derive(Parser)]
#[grammar = "datalog.pest"]
pub struct DatalogParser;

pub fn parse_query(query_str: &str) -> Result<Query, MesoError> {
    let mut parsed = DatalogParser::parse(Rule::query, query_str)
        .map_err(|e| MesoError::ParseError(e.to_string()))?;

    let query_pair = parsed.next().unwrap();
    let mut query = Query::default();

    for pair in query_pair.into_inner() {
        match pair.as_rule() {
            Rule::find_clause => {
                for find_elem in pair.into_inner() {
                    let inner = find_elem.into_inner().next().unwrap();
                    match inner.as_rule() {
                        Rule::variable => {
                            query
                                .find
                                .push(FindSpec::Variable(inner.as_str().to_string()));
                        }
                        Rule::pull_expr => {
                            let mut pull_inner = inner.into_inner();
                            let var_name = pull_inner.next().unwrap().as_str().to_string();
                            let pattern = parse_pull_pattern(pull_inner.next().unwrap());
                            query.find.push(FindSpec::Pull(var_name, pattern));
                        }
                        Rule::aggr_expr => {
                            let mut aggr_inner = inner.into_inner();
                            let aggr_name = aggr_inner.next().unwrap().as_str().to_string();
                            let var_name = aggr_inner.next().unwrap().as_str().to_string();
                            query.find.push(FindSpec::Aggregate(aggr_name, var_name));
                        }
                        _ => {}
                    }
                }
            }
            Rule::in_clause => {
                let mut in_vars = Vec::new();
                for in_elem in pair.into_inner() {
                    let inner = in_elem.into_inner().next().unwrap();
                    match inner.as_rule() {
                        Rule::variable => {
                            in_vars.push(InSpec::Variable(inner.as_str().to_string()))
                        }
                        Rule::data_src => {
                            in_vars.push(InSpec::DataSource(inner.as_str().to_string()))
                        }
                        _ => {}
                    }
                }
                query.in_vars = Some(in_vars);
            }
            Rule::where_clause => {
                for where_elem in pair.into_inner() {
                    let inner = where_elem.into_inner().next().unwrap();
                    match inner.as_rule() {
                        Rule::data_pattern => {
                            let mut terms = Vec::new();
                            let mut options = None;

                            for part in inner.into_inner() {
                                if part.as_rule() == Rule::options_map {
                                    options = Some(parse_options_map(part));
                                } else {
                                    terms.push(parse_term(part));
                                }
                            }

                            if terms.len() >= 2 {
                                let e = terms[0].clone();
                                let a = terms[1].clone();
                                let v = terms.get(2).cloned().unwrap_or(Term::Blank);
                                let tx = terms.get(3).cloned();

                                query.where_clauses.push(WhereClause::DataPattern {
                                    e,
                                    a,
                                    v,
                                    tx,
                                    options,
                                });
                            }
                        }
                        Rule::rule_expr => {
                            let mut rule_inner = inner.into_inner();
                            let rule_name = rule_inner.next().unwrap().as_str().to_string();
                            let args = rule_inner.map(parse_term).collect();
                            query
                                .where_clauses
                                .push(WhereClause::RuleExpr { rule_name, args });
                        }
                        Rule::fn_clause => {
                            let mut fn_inner = inner.into_inner();
                            let fn_expr = fn_inner.next().unwrap();
                            let binding_expr = fn_inner.next().unwrap();

                            let mut fn_args = fn_expr.into_inner();
                            let fn_name = fn_args.next().unwrap().as_str().to_string();
                            let args = fn_args.map(parse_term).collect();

                            let binding = parse_binding(binding_expr);

                            query.where_clauses.push(WhereClause::Function {
                                fn_name,
                                args,
                                binding,
                            });
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    Ok(query)
}

fn parse_pull_pattern(pair: pest::iterators::Pair<Rule>) -> PullPattern {
    let mut attrs = Vec::new();
    for pull_attr in pair.into_inner() {
        let inner = pull_attr.into_inner().next().unwrap();
        match inner.as_rule() {
            Rule::wildcard => {
                attrs.push(PullAttribute::Wildcard);
            }
            Rule::keyword => {
                attrs.push(PullAttribute::Simple(inner.as_str().to_string()));
            }
            Rule::pull_map => {
                let mut map_inner = inner.into_inner();
                let key = map_inner.next().unwrap().as_str().to_string();
                let sub_pattern = parse_pull_pattern(map_inner.next().unwrap());
                attrs.push(PullAttribute::Map(key, sub_pattern));
            }
            _ => {}
        }
    }
    PullPattern(attrs)
}

fn parse_term(pair: pest::iterators::Pair<Rule>) -> Term {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::variable => Term::Variable(inner.as_str().to_string()),
        Rule::keyword => Term::Keyword(inner.as_str().to_string()),
        Rule::string => {
            let s = inner.into_inner().next().map(|p| p.as_str()).unwrap_or("");
            Term::String(s.to_string())
        }
        Rule::integer => Term::Integer(inner.as_str().parse().unwrap()),
        Rule::float => Term::Float(inner.as_str().parse().unwrap()),
        Rule::boolean => Term::Boolean(inner.as_str() == "true"),
        Rule::blank => Term::Blank,
        Rule::data_src => Term::DataSource(inner.as_str().to_string()),
        _ => Term::Blank,
    }
}

fn parse_options_map(pair: pest::iterators::Pair<Rule>) -> BTreeMap<String, Term> {
    let mut map = BTreeMap::new();
    for entry in pair.into_inner() {
        let mut entry_inner = entry.into_inner();

        // FIX: Just call .as_str() directly on the option_key pair to keep the ":" prefix
        let key = entry_inner.next().unwrap().as_str().to_string();

        let val_pair = entry_inner.next().unwrap().into_inner().next().unwrap();
        let val = if val_pair.as_rule() == Rule::vector_2 {
            let mut vec_inner = val_pair.into_inner();
            let t1 = parse_term(vec_inner.next().unwrap());
            let t2 = parse_term(vec_inner.next().unwrap());
            Term::Vector(vec![t1, t2])
        } else {
            parse_term(val_pair)
        };

        map.insert(key, val);
    }
    map
}

fn parse_binding(pair: pest::iterators::Pair<Rule>) -> Binding {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::binding_scalar => {
            let var = inner.into_inner().next().unwrap().as_str().to_string();
            Binding::Scalar(var)
        }
        Rule::binding_tuple => {
            let vars = inner.into_inner().map(|p| p.as_str().to_string()).collect();
            Binding::Tuple(vars)
        }
        Rule::binding_rel => {
            let vars = inner.into_inner().map(|p| p.as_str().to_string()).collect();
            Binding::Relation(vars)
        }
        _ => Binding::Scalar("?unknown".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_id_0() {
        let q = r#"[:find ?name :where [?e :user/name ?name]]"#;
        let ast = parse_query(q).unwrap();
        assert_eq!(ast.find, vec![FindSpec::Variable("?name".into())]);
        assert_eq!(ast.where_clauses.len(), 1);
    }

    #[test]
    fn test_parse_basic_id_1() {
        let q = r#"[:find ?e :where [?e :meso/id 1001]]"#;
        let ast = parse_query(q).unwrap();
        assert_eq!(ast.find.len(), 1);
        if let WhereClause::DataPattern { a, v, .. } = &ast.where_clauses[0] {
            assert_eq!(a, &Term::Keyword(":meso/id".into()));
            assert_eq!(v, &Term::Integer(1001));
        } else {
            panic!("Expected DataPattern");
        }
    }

    #[test]
    fn test_parse_basic_id_2_with_in() {
        let q = r#"[:find ?e :in $ ?id :where [?e :meso/id ?id]]"#;
        let ast = parse_query(q).unwrap();
        let in_vars = ast.in_vars.as_ref().unwrap();
        assert_eq!(in_vars[0], InSpec::DataSource("$".into()));
        assert_eq!(in_vars[1], InSpec::Variable("?id".into()));
    }

    #[test]
    fn test_parse_predicate_string() {
        let q = r#"[:find ?e :where [?e :user/email "alice@test.com"]]"#;
        let ast = parse_query(q).unwrap();
        if let WhereClause::DataPattern { v, .. } = &ast.where_clauses[0] {
            assert_eq!(v, &Term::String("alice@test.com".into()));
        }
    }

    #[test]
    fn test_parse_pull_simple() {
        let q = r#"[:find (pull ?e [:user/name :user/email]) :where [?e :meso/id 1]]"#;
        let ast = parse_query(q).unwrap();
        if let FindSpec::Pull(var, pat) = &ast.find[0] {
            assert_eq!(var, "?e");
            assert_eq!(pat.0.len(), 2);
        }
    }

    #[test]
    fn test_parse_jit_temporal_at() {
        let q = r#"[:find ?n :where [?e :user/name ?n {:at 1600000000}]]"#;
        let ast = parse_query(q).unwrap();
        if let WhereClause::DataPattern { options, .. } = &ast.where_clauses[0] {
            let opts = options.as_ref().expect("Options map should exist");
            assert_eq!(opts.get(":at"), Some(&Term::Integer(1600000000)));
        } else {
            panic!("Expected DataPattern with options");
        }
    }

    #[test]
    fn test_parse_jit_temporal_since_with_tx() {
        let q = r#"[:find ?e :where [?e :user/age _ 101 {:since "2024-01-01"}]]"#;
        let ast = parse_query(q).unwrap();
        if let WhereClause::DataPattern { v, tx, options, .. } = &ast.where_clauses[0] {
            assert_eq!(v, &Term::Blank);
            assert_eq!(tx.as_ref().unwrap(), &Term::Integer(101));
            let opts = options.as_ref().unwrap();
            assert_eq!(opts.get(":since"), Some(&Term::String("2024-01-01".into())));
        }
    }

    #[test]
    fn test_parse_jit_temporal_between_vector() {
        let q = r#"[:find ?e :where [?e :user/login _ {:between [1000 2000]}]]"#;
        let ast = parse_query(q).unwrap();
        if let WhereClause::DataPattern { options, .. } = &ast.where_clauses[0] {
            let opts = options.as_ref().unwrap();
            if let Some(Term::Vector(v)) = opts.get(":between") {
                assert_eq!(v.len(), 2);
                assert_eq!(v[0], Term::Integer(1000));
                assert_eq!(v[1], Term::Integer(2000));
            } else {
                panic!("Expected Vector for :between");
            }
        }
    }

    #[test]
    fn test_parse_xtdb_style_short_pattern() {
        let q = r#"[:find ?e :where [?e :user/active {:at "2023-05-01"}]]"#;
        let ast = parse_query(q).unwrap();
        if let WhereClause::DataPattern { v, options, .. } = &ast.where_clauses[0] {
            assert_eq!(v, &Term::Blank);
            let opts = options.as_ref().unwrap();
            assert_eq!(opts.get(":at"), Some(&Term::String("2023-05-01".into())));
        }
    }

    #[test]
    fn test_parse_multiple_options() {
        let q = r#"[:find ?e :where [?e :user/name ?n {:at 500 :since 100}]]"#;
        let ast = parse_query(q).unwrap();
        if let WhereClause::DataPattern { options, .. } = &ast.where_clauses[0] {
            let opts = options.as_ref().unwrap();
            assert_eq!(opts.len(), 2);
            assert!(opts.contains_key(":at"));
            assert!(opts.contains_key(":since"));
        }
    }

    #[test]
    fn test_parse_fail_on_nested_map() {
        let q = r#"[:find ?e :where [?e :a ?v {:outer {:inner 1}}]]"#;
        let result = parse_query(q);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_datomic_pull_syntax() {
        let q = r#"
            [:find (pull ?order [:order/id
                                 {:order/customer [:customer/name]}
                                 {:order/items [:item/qty {:item/product [*]}]}])
             :where [?order :order/id "123"]]
        "#;

        let ast = parse_query(q).unwrap();

        let find_term = &ast.find[0];
        if let FindSpec::Pull(var, pattern) = find_term {
            assert_eq!(var, "?order");
            assert_eq!(pattern.0.len(), 3);

            assert_eq!(pattern.0[0], PullAttribute::Simple(":order/id".into()));

            assert_eq!(
                pattern.0[1],
                PullAttribute::Map(
                    ":order/customer".into(),
                    PullPattern(vec![PullAttribute::Simple(":customer/name".into())])
                )
            );

            assert_eq!(
                pattern.0[2],
                PullAttribute::Map(
                    ":order/items".into(),
                    PullPattern(vec![
                        PullAttribute::Simple(":item/qty".into()),
                        PullAttribute::Map(
                            ":item/product".into(),
                            PullPattern(vec![PullAttribute::Wildcard])
                        )
                    ])
                )
            );
        } else {
            panic!("Expected Pull term");
        }
    }
}
