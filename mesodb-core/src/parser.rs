// mesodb-core/src/parser.rs

use pest::Parser;
use pest_derive::Parser;
use thiserror::Error;

use crate::ast::{Binding, FindSpec, InSpec, PullAttribute, PullPattern, Query, Term, WhereClause};

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Failed to parse query: {0}")]
    InvalidSyntax(String),
}

#[derive(Parser)]
#[grammar = "datalog.pest"]
pub struct DatalogParser;

pub fn parse_query(raw_query: &str) -> Result<Query, ParseError> {
    let mut parsed = DatalogParser::parse(Rule::query, raw_query)
        .map_err(|e| ParseError::InvalidSyntax(e.to_string()))?;

    let query_pair = parsed.next().unwrap();

    let mut find = Vec::new();
    let mut in_vars = None;
    let mut where_clauses = Vec::new();

    for inner_pair in query_pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::find_clause => {
                for find_elem in inner_pair.into_inner() {
                    let elem_inner = find_elem.into_inner().next().unwrap();
                    match elem_inner.as_rule() {
                        Rule::variable => {
                            find.push(FindSpec::Variable(elem_inner.as_str().to_string()))
                        }
                        Rule::pull_expr => {
                            let mut pull_parts = elem_inner.into_inner();
                            let var = pull_parts.next().unwrap().as_str().to_string();
                            let pat = parse_pull_pattern(pull_parts.next().unwrap());
                            find.push(FindSpec::Pull(var, pat));
                        }
                        Rule::aggr_expr => {
                            let mut aggr_parts = elem_inner.into_inner();
                            let func = aggr_parts.next().unwrap().as_str().to_string();
                            let var = aggr_parts.next().unwrap().as_str().to_string();
                            find.push(FindSpec::Aggregate(func, var));
                        }
                        _ => unreachable!(),
                    }
                }
            }
            Rule::in_clause => {
                let mut ins = Vec::new();
                for in_elem in inner_pair.into_inner() {
                    let elem_inner = in_elem.into_inner().next().unwrap();
                    match elem_inner.as_rule() {
                        Rule::data_src => {
                            ins.push(InSpec::DataSource(elem_inner.as_str().to_string()))
                        }
                        Rule::variable => {
                            ins.push(InSpec::Variable(elem_inner.as_str().to_string()))
                        }
                        _ => unreachable!(),
                    }
                }
                in_vars = Some(ins);
            }
            Rule::where_clause => {
                for where_elem in inner_pair.into_inner() {
                    let elem_inner = where_elem.into_inner().next().unwrap();
                    match elem_inner.as_rule() {
                        Rule::data_pattern => {
                            let mut terms = Vec::new();
                            for term_pair in elem_inner.into_inner() {
                                terms.push(parse_term(term_pair));
                            }
                            where_clauses.push(WhereClause::DataPattern {
                                e: terms[0].clone(),
                                a: terms[1].clone(),
                                v: terms[2].clone(),
                                tx: terms.get(3).cloned(),
                            });
                        }
                        Rule::rule_expr => {
                            let mut rule_parts = elem_inner.into_inner();
                            let rule_name = rule_parts.next().unwrap().as_str().to_string();
                            let mut args = Vec::new();
                            for arg_pair in rule_parts {
                                args.push(parse_term(arg_pair));
                            }
                            where_clauses.push(WhereClause::RuleExpr { rule_name, args });
                        }
                        // NEW: Map function clauses
                        Rule::fn_clause => {
                            let mut fn_parts = elem_inner.into_inner();
                            let expr_pair = fn_parts.next().unwrap();
                            let bind_pair = fn_parts.next().unwrap();

                            // 1. Extract the function name and arguments
                            let mut expr_inner = expr_pair.into_inner();
                            let fn_name = expr_inner.next().unwrap().as_str().to_string();
                            let mut args = Vec::new();
                            for arg_pair in expr_inner {
                                args.push(parse_term(arg_pair));
                            }

                            // 2. Extract the binding strategy
                            let bind_inner = bind_pair.into_inner().next().unwrap();
                            let binding = match bind_inner.as_rule() {
                                Rule::binding_scalar => {
                                    Binding::Scalar(bind_inner.as_str().to_string())
                                }
                                Rule::binding_tuple => {
                                    let vars = bind_inner
                                        .into_inner()
                                        .map(|v| v.as_str().to_string())
                                        .collect();
                                    Binding::Tuple(vars)
                                }
                                Rule::binding_rel => {
                                    let vars = bind_inner
                                        .into_inner()
                                        .map(|v| v.as_str().to_string())
                                        .collect();
                                    Binding::Relation(vars)
                                }
                                _ => unreachable!(),
                            };

                            where_clauses.push(WhereClause::Function {
                                fn_name,
                                args,
                                binding,
                            });
                        }
                        _ => unreachable!(),
                    }
                }
            }
            _ => {}
        }
    }

    Ok(Query {
        find,
        in_vars,
        where_clauses,
    })
}

fn parse_term(pair: pest::iterators::Pair<Rule>) -> Term {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::variable => Term::Variable(inner.as_str().to_string()),
        Rule::keyword => Term::Keyword(inner.as_str().to_string()),
        Rule::integer => Term::Integer(inner.as_str().parse().unwrap()),
        Rule::float => Term::Float(inner.as_str().parse().unwrap()),
        Rule::boolean => Term::Boolean(inner.as_str() == "true"),
        Rule::string => Term::String(inner.into_inner().next().unwrap().as_str().to_string()),
        Rule::blank => Term::Blank,
        Rule::data_src => Term::DataSource(inner.as_str().to_string()),
        _ => unreachable!(),
    }
}

fn parse_pull_pattern(pair: pest::iterators::Pair<Rule>) -> PullPattern {
    let mut attrs = Vec::new();
    for pull_attr in pair.into_inner() {
        let inner = pull_attr.into_inner().next().unwrap();
        match inner.as_rule() {
            Rule::keyword => attrs.push(PullAttribute::Simple(inner.as_str().to_string())),
            Rule::pull_map => {
                let mut map_inner = inner.into_inner();
                let kw = map_inner.next().unwrap().as_str().to_string();
                let pat = parse_pull_pattern(map_inner.next().unwrap());
                attrs.push(PullAttribute::Map(kw, pat));
            }
            _ => unreachable!(),
        }
    }
    PullPattern(attrs)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Group 1: Basic ID Queries ---
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
        let in_vars = ast.in_vars.unwrap();
        assert_eq!(in_vars[0], InSpec::DataSource("$".into()));
        assert_eq!(in_vars[1], InSpec::Variable("?id".into()));
    }

    // --- Group 2: Predicates (Int, Float, String, Blank) ---
    #[test]
    fn test_parse_predicate_string() {
        let q = r#"[:find ?e :where [?e :user/email "alice@test.com"]]"#;
        let ast = parse_query(q).unwrap();
        if let WhereClause::DataPattern { v, .. } = &ast.where_clauses[0] {
            assert_eq!(v, &Term::String("alice@test.com".into()));
        } else {
            panic!("Expected DataPattern");
        }
    }

    #[test]
    fn test_parse_predicate_float() {
        let q = r#"[:find ?e :where [?e :math/pi 4.14159]]"#;
        let ast = parse_query(q).unwrap();
        if let WhereClause::DataPattern { v, .. } = &ast.where_clauses[0] {
            assert_eq!(v, &Term::Float(4.14159));
        } else {
            panic!("Expected DataPattern");
        }
    }

    #[test]
    fn test_parse_predicate_blank_node() {
        let q = r#"[:find ?e :where [?e :user/email _]]"#;
        let ast = parse_query(q).unwrap();
        if let WhereClause::DataPattern { v, .. } = &ast.where_clauses[0] {
            assert_eq!(v, &Term::Blank);
        } else {
            panic!("Expected DataPattern");
        }
    }

    // --- Group 3: Pull Syntax ---
    #[test]
    fn test_parse_pull_simple() {
        let q = r#"[:find (pull ?e [:user/name :user/email]) :where [?e :meso/id 1]]"#;
        let ast = parse_query(q).unwrap();
        if let FindSpec::Pull(var, pat) = &ast.find[0] {
            assert_eq!(var, "?e");
            assert_eq!(pat.0.len(), 2);
            assert_eq!(pat.0[0], PullAttribute::Simple(":user/name".into()));
        } else {
            panic!("Expected Pull");
        }
    }

    #[test]
    fn test_parse_pull_nested_single() {
        let q = r#"[:find (pull ?e [:user/name {:user/address [:address/street]}]) :where [?e :meso/id 1]]"#;
        let ast = parse_query(q).unwrap();
        if let FindSpec::Pull(_, pat) = &ast.find[0] {
            assert_eq!(pat.0.len(), 2);
            if let PullAttribute::Map(kw, sub_pat) = &pat.0[1] {
                assert_eq!(kw, ":user/address");
                assert_eq!(
                    sub_pat.0[0],
                    PullAttribute::Simple(":address/street".into())
                );
            } else {
                panic!("Expected Nested Map");
            }
        } else {
            panic!("Expected Pull");
        }
    }

    #[test]
    fn test_parse_pull_nested_multiple() {
        let q = r#"[:find (pull ?e [{:user/address [:address/street :address/city]} {:user/employer [:employer/name]}]) :where [?e :meso/id 1]]"#;
        let ast = parse_query(q).unwrap();
        if let FindSpec::Pull(_, pat) = &ast.find[0] {
            assert_eq!(pat.0.len(), 2);
        } else {
            panic!("Expected Pull");
        }
    }

    // --- Group 4: User Input (:in) ---
    #[test]
    fn test_parse_in_multiple() {
        let q = r#"[:find ?name :in $ ?age ?status :where [?e :user/age ?age] [?e :user/status ?status]]"#;
        let ast = parse_query(q).unwrap();
        let in_vars = ast.in_vars.unwrap();
        assert_eq!(in_vars.len(), 3);
        assert_eq!(in_vars[2], InSpec::Variable("?status".into()));
    }

    // --- Group 5: Joins Across Namespaces ---
    #[test]
    fn test_parse_2_hop_join() {
        let q = r#"[:find ?street :where [?e :user/name "Alice"] [?e :user/address ?a] [?a :address/street ?street]]"#;
        let ast = parse_query(q).unwrap();
        assert_eq!(ast.where_clauses.len(), 3);
    }

    #[test]
    fn test_parse_3_hop_join() {
        let q = r#"
            [:find ?employer
             :where [?u :user/name "Bob"]
                    [?u :user/address ?a]
                    [?a :address/city "NYC"]
                    [?u :user/employer ?emp]
                    [?emp :employer/name ?employer]]
        "#;
        let ast = parse_query(q).unwrap();
        assert_eq!(ast.where_clauses.len(), 5);
    }

    #[test]
    fn test_parse_self_join() {
        let q = r#"[:find ?friend_name :where [?u :user/name "Alice"] [?u :user/friend ?f] [?f :user/name ?friend_name]]"#;
        let ast = parse_query(q).unwrap();
        assert_eq!(ast.where_clauses.len(), 3);
    }

    // --- Group 6: Rules ---
    #[test]
    fn test_parse_simple_rule() {
        let q = r#"[:find ?e :in $ % :where (active-user ?e)]"#;
        let ast = parse_query(q).unwrap();
        assert_eq!(ast.in_vars.unwrap()[1], InSpec::DataSource("%".into()));

        if let WhereClause::RuleExpr { rule_name, args } = &ast.where_clauses[0] {
            assert_eq!(rule_name, "active-user");
            assert_eq!(args[0], Term::Variable("?e".into()));
        } else {
            panic!("Expected RuleExpr");
        }
    }

    #[test]
    fn test_parse_rule_with_args() {
        let q = r#"[:find ?e :in $ % ?status :where (users-by-status ?e ?status)]"#;
        let ast = parse_query(q).unwrap();
        if let WhereClause::RuleExpr { args, .. } = &ast.where_clauses[0] {
            assert_eq!(args.len(), 2);
        } else {
            panic!("Expected RuleExpr");
        }
    }

    // --- Group 7: Aggregations & Misc ---
    #[test]
    fn test_parse_aggregate_count() {
        let q = r#"[:find (count ?e) :where [?e :user/active true]]"#;
        let ast = parse_query(q).unwrap();
        if let FindSpec::Aggregate(func, var) = &ast.find[0] {
            assert_eq!(func, "count");
            assert_eq!(var, "?e");
        } else {
            panic!("Expected Aggregate");
        }

        if let WhereClause::DataPattern { v, .. } = &ast.where_clauses[0] {
            assert_eq!(v, &Term::Boolean(true));
        } else {
            panic!("Expected DataPattern");
        }
    }

    #[test]
    fn test_parse_data_pattern_with_tx() {
        // 4-tuple including the transaction ID
        let q = r#"[:find ?e ?tx :where [?e :user/name "Alice" ?tx]]"#;
        let ast = parse_query(q).unwrap();
        if let WhereClause::DataPattern { tx, .. } = &ast.where_clauses[0] {
            assert_eq!(tx.as_ref().unwrap(), &Term::Variable("?tx".into()));
        } else {
            panic!("Expected tx to be parsed");
        }
    }

    #[test]
    fn test_parse_fulltext_search_relation_binding() {
        // [[]] indicates the function returns rows (e.g. Entity and Score)
        let q =
            r#"[:find ?e ?score :where [(fulltext $ :user/bio "rust database") [[?e ?score]]]]"#;
        let ast = parse_query(q).unwrap();

        if let WhereClause::Function {
            fn_name,
            args,
            binding,
        } = &ast.where_clauses[0]
        {
            assert_eq!(fn_name, "fulltext");
            assert_eq!(args[0], Term::DataSource("$".into()));
            assert_eq!(args[1], Term::Keyword(":user/bio".into()));
            assert_eq!(args[2], Term::String("rust database".into()));

            if let Binding::Relation(vars) = binding {
                assert_eq!(vars, &vec!["?e".to_string(), "?score".to_string()]);
            } else {
                panic!("Expected Relation binding");
            }
        } else {
            panic!("Expected Function clause");
        }
    }

    #[test]
    fn test_parse_function_tuple_binding() {
        // [] indicates the function returns a single tuple
        let q = r#"[:find ?lat ?lng :where [(get-coordinates ?e) [?lat ?lng]]]"#;
        let ast = parse_query(q).unwrap();

        if let WhereClause::Function { binding, .. } = &ast.where_clauses[0] {
            if let Binding::Tuple(vars) = binding {
                assert_eq!(vars, &vec!["?lat".to_string(), "?lng".to_string()]);
            } else {
                panic!("Expected Tuple binding");
            }
        } else {
            panic!("Expected Function clause");
        }
    }

    #[test]
    fn test_parse_function_scalar_binding() {
        // Raw variable indicates the function returns a single scalar value
        let q = r#"[:find ?c :where [(add ?a ?b) ?c]]"#;
        let ast = parse_query(q).unwrap();

        if let WhereClause::Function { binding, .. } = &ast.where_clauses[0] {
            if let Binding::Scalar(var) = binding {
                assert_eq!(var, "?c");
            } else {
                panic!("Expected Scalar binding");
            }
        } else {
            panic!("Expected Function clause");
        }
    }
}
