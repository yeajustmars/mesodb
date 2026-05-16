// mesodb-core/src/ast.rs
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    Variable(String),
    Keyword(String),
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Blank,
    DataSource(String),
    Vector(Vec<Term>), // Added for :between [start end]
}

#[derive(Debug, Clone, PartialEq)]
pub enum Binding {
    Scalar(String),
    Tuple(Vec<String>),
    Relation(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum PullAttribute {
    Wildcard, // <-- ADDED: Supports [*]
    Simple(String),
    Map(String, PullPattern),
}

#[derive(Debug, Clone, PartialEq)]
pub struct PullPattern(pub Vec<PullAttribute>);

#[derive(Debug, Clone, PartialEq)]
pub enum FindSpec {
    Variable(String),
    Pull(String, PullPattern),
    Aggregate(String, String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum InSpec {
    DataSource(String),
    Variable(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum WhereClause {
    DataPattern {
        e: Term,
        a: Term,
        v: Term,
        tx: Option<Term>,
        options: Option<BTreeMap<String, Term>>,
    },
    RuleExpr {
        rule_name: String,
        args: Vec<Term>,
    },
    Function {
        fn_name: String,
        args: Vec<Term>,
        binding: Binding,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleHead {
    pub name: String,
    pub args: Vec<String>, // The variables, e.g., ["?child", "?ancestor"]
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleDef {
    pub head: RuleHead,
    pub body: Vec<WhereClause>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct RuleSet {
    pub rules: Vec<RuleDef>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Query {
    pub find: Vec<FindSpec>,
    pub in_vars: Option<Vec<InSpec>>,
    pub where_clauses: Vec<WhereClause>,
}
