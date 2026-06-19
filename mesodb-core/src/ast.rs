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
    DataSource(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Binding {
    Scalar(String),
    Tuple(Vec<String>),
    Relation(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum PullAttribute {
    Wildcard,
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
    Function {
        fn_name: String,
        args: Vec<Term>,
        binding: Option<Binding>,
    },
    Or {
        join_vars: Option<Vec<String>>,
        clauses: Vec<WhereClause>,
    },
    Not {
        join_vars: Option<Vec<String>>,
        clauses: Vec<WhereClause>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleHead {
    pub name: String,
    pub args: Vec<String>,
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
