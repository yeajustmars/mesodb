// mesodb-core/src/parser.rs
use pest::Parser;
use pest_derive::Parser;
use std::collections::BTreeMap;
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
                            let mut inner = elem_inner.into_inner();

                            let e = parse_term(inner.next().ok_or_else(|| {
                                ParseError::InvalidSyntax("Missing Entity".into())
                            })?);
                            let a = parse_term(inner.next().ok_or_else(|| {
                                ParseError::InvalidSyntax("Missing Attribute".into())
                            })?);

                            let mut v = Term::Blank;
                            let mut tx = None;
                            let mut options = None;
                            let mut extra_terms = Vec::new();

                            for pair in inner {
                                match pair.as_rule() {
                                    Rule::term => {
                                        extra_terms.push(parse_term(pair));
                                    }
                                    Rule::options_map => {
                                        let mut opts = BTreeMap::new();
                                        for entry in pair.into_inner() {
                                            let mut entry_inner = entry.into_inner();
                                            let key_pair = entry_inner.next().unwrap();
                                            let key = key_pair.as_str().to_string();

                                            let val_wrapper = entry_inner.next().unwrap(); // This is the 'option_val' rule
                                            let val_inner =
                                                val_wrapper.into_inner().next().unwrap(); // Peel to 'term' or 'vector_2'

                                            let val = match val_inner.as_rule() {
                                                Rule::vector_2 => {
                                                    let mut v_inner = val_inner.into_inner();
                                                    Term::Vector(vec![
                                                        parse_term(v_inner.next().unwrap()),
                                                        parse_term(v_inner.next().unwrap()),
                                                    ])
                                                }
                                                Rule::term => parse_term(val_inner),
                                                _ => unreachable!(
                                                    "Unexpected rule inside option_val: {:?}",
                                                    val_inner.as_rule()
                                                ),
                                            };
                                            opts.insert(key, val);
                                        }
                                        options = Some(opts);
                                    }
                                    _ => {}
                                }
                            }

                            if let Some(first) = extra_terms.get(0) {
                                v = first.clone();
                            }
                            if let Some(second) = extra_terms.get(1) {
                                tx = Some(second.clone());
                            }

                            where_clauses.push(WhereClause::DataPattern {
                                e,
                                a,
                                v,
                                tx,
                                options,
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
                        Rule::fn_clause => {
                            let mut fn_parts = elem_inner.into_inner();
                            let expr_pair = fn_parts.next().unwrap();
                            let bind_pair = fn_parts.next().unwrap();
                            let mut expr_inner = expr_pair.into_inner();
                            let fn_name = expr_inner.next().unwrap().as_str().to_string();
                            let mut args = Vec::new();
                            for arg_pair in expr_inner {
                                args.push(parse_term(arg_pair));
                            }
                            let bind_inner = bind_pair.into_inner().next().unwrap();
                            let binding = match bind_inner.as_rule() {
                                Rule::binding_scalar => {
                                    Binding::Scalar(bind_inner.as_str().to_string())
                                }
                                Rule::binding_tuple => Binding::Tuple(
                                    bind_inner
                                        .into_inner()
                                        .map(|v| v.as_str().to_string())
                                        .collect(),
                                ),
                                Rule::binding_rel => Binding::Relation(
                                    bind_inner
                                        .into_inner()
                                        .map(|v| v.as_str().to_string())
                                        .collect(),
                                ),
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
    let inner = if pair.as_rule() == Rule::term {
        pair.into_inner()
            .next()
            .expect("Term wrapper must have a primitive inside")
    } else {
        pair
    };

    match inner.as_rule() {
        Rule::variable => Term::Variable(inner.as_str().to_string()),
        Rule::keyword => Term::Keyword(inner.as_str().to_string()),
        Rule::integer => Term::Integer(inner.as_str().parse().unwrap()),
        Rule::float => Term::Float(inner.as_str().parse().unwrap()),
        Rule::boolean => Term::Boolean(inner.as_str() == "true"),
        Rule::string => Term::String(inner.into_inner().next().unwrap().as_str().to_string()),
        Rule::blank => Term::Blank,
        Rule::data_src => Term::DataSource(inner.as_str().to_string()),
        _ => unreachable!("parse_term got unexpected rule: {:?}", inner.as_rule()),
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
    use crate::ast::*;

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
}
