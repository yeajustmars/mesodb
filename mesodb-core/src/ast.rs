// mesodb-core/src/ast.rs

#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    Variable(String),
    Keyword(String),
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Blank,
    DataSource(String), // NEW: e.g., "$" or "%"
}

// NEW: Represents how a function output binds to variables
#[derive(Debug, Clone, PartialEq)]
pub enum Binding {
    Scalar(String),        // e.g., ?c
    Tuple(Vec<String>),    // e.g., [?x ?y]
    Relation(Vec<String>), // e.g., [[?e ?score]]
}

#[derive(Debug, Clone, PartialEq)]
pub enum PullAttribute {
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
    },
    RuleExpr {
        rule_name: String,
        args: Vec<Term>,
    },
    // NEW: The AST node for fulltext search and other functions
    Function {
        fn_name: String,
        args: Vec<Term>,
        binding: Binding,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub find: Vec<FindSpec>,
    pub in_vars: Option<Vec<InSpec>>,
    pub where_clauses: Vec<WhereClause>,
}
