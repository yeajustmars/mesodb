// mesodb-core/src/edn.rs

use chrono::DateTime;
use std::iter::Peekable;
use std::str::Chars;

use crate::error::MesoError;
use crate::transactor::Fact;
use crate::types::{Result, Value};

#[derive(Debug, PartialEq)]
enum RawToken<'a> {
    Keyword(&'a str),
    String(&'a str),
    Int(i64),
    Float(f64),
    Inst(i64),
    Symbol(&'a str),
    LeftBracket,
    RightBracket,
}

struct EdnLexer<'a> {
    input: &'a str,
    chars: Peekable<Chars<'a>>,
    byte_idx: usize,
}

impl<'a> EdnLexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            chars: input.chars().peekable(),
            byte_idx: 0,
        }
    }

    fn next_char(&mut self) -> Option<char> {
        if let Some(c) = self.chars.next() {
            self.byte_idx += c.len_utf8();
            Some(c)
        } else {
            None
        }
    }

    fn peek_char(&mut self) -> Option<&char> {
        self.chars.peek()
    }

    fn skip_whitespace_and_commas(&mut self) {
        while let Some(&c) = self.peek_char() {
            if c.is_whitespace() || c == ',' {
                self.next_char();
            } else {
                break;
            }
        }
    }

    fn next_token(&mut self) -> Result<Option<RawToken<'a>>> {
        self.skip_whitespace_and_commas();

        let start_char = match self.peek_char() {
            Some(&c) => c,
            None => return Ok(None),
        };

        match start_char {
            '[' => {
                self.next_char();
                Ok(Some(RawToken::LeftBracket))
            }
            ']' => {
                self.next_char();
                Ok(Some(RawToken::RightBracket))
            }
            '"' => {
                self.next_char(); // Consume opening quote
                let start = self.byte_idx;
                while let Some(c) = self.next_char() {
                    if c == '"' {
                        let end = self.byte_idx - 1;
                        return Ok(Some(RawToken::String(&self.input[start..end])));
                    }
                }
                Err(MesoError::Serialization(
                    "Unterminated string literal".into(),
                ))
            }
            '#' => {
                self.next_char(); // Consume '#'
                let mut tag = String::new();
                while let Some(&c) = self.peek_char() {
                    if c.is_whitespace() || c == '"' || c == '[' || c == ']' {
                        break;
                    }
                    tag.push(self.next_char().unwrap());
                }

                if tag == "inst" {
                    self.skip_whitespace_and_commas();
                    if self.next_char() != Some('"') {
                        return Err(MesoError::Serialization(
                            "Expected string literal after #inst".into(),
                        ));
                    }
                    let start = self.byte_idx;
                    while let Some(c) = self.next_char() {
                        if c == '"' {
                            let end = self.byte_idx - 1;
                            let dt_str = &self.input[start..end];
                            let dt = DateTime::parse_from_rfc3339(dt_str).map_err(|e| {
                                MesoError::Serialization(format!("Invalid #inst format: {e}"))
                            })?;
                            return Ok(Some(RawToken::Inst(dt.timestamp_micros())));
                        }
                    }
                    Err(MesoError::Serialization(
                        "Unterminated #inst literal".into(),
                    ))
                } else {
                    Err(MesoError::Serialization(format!("Unknown tag: #{tag}")))
                }
            }
            _ => {
                // Parse numbers, keywords, and symbols via slice bounds matching
                let start = self.byte_idx;
                while let Some(&c) = self.peek_char() {
                    if c.is_whitespace() || c == ',' || c == '[' || c == ']' || c == '"' {
                        break;
                    }
                    self.next_char();
                }
                let end = self.byte_idx;
                let slice = &self.input[start..end];

                if slice.starts_with(':') {
                    Ok(Some(RawToken::Keyword(slice)))
                } else if let Ok(i) = slice.parse::<i64>() {
                    Ok(Some(RawToken::Int(i)))
                } else if let Ok(f) = slice.parse::<f64>() {
                    Ok(Some(RawToken::Float(f)))
                } else {
                    Ok(Some(RawToken::Symbol(slice)))
                }
            }
        }
    }
}

pub fn parse_transaction(input: &str) -> Result<Vec<Fact>> {
    let mut lexer = EdnLexer::new(input);

    // Outer boundary must be a vector enclosure
    match lexer.next_token()? {
        Some(RawToken::LeftBracket) => {}
        _ => {
            return Err(MesoError::Serialization(
                "Transaction input must be enclosed in an outer transaction bracket '['".into(),
            ));
        }
    }

    let mut facts = Vec::new();

    loop {
        lexer.skip_whitespace_and_commas();
        match lexer.peek_char() {
            Some(']') => {
                lexer.next_char(); // Consume final closing bracket
                break;
            }
            None => {
                return Err(MesoError::Serialization(
                    "Missing closing bracket ']' for transaction collection".into(),
                ));
            }
            _ => match lexer.next_token()? {
                Some(RawToken::LeftBracket) => {
                    let fact = parse_fact_vector(&mut lexer)?;
                    facts.push(fact);
                }
                _ => {
                    return Err(MesoError::Serialization(
                        "Expected inner fact assertion vector starting with '['".into(),
                    ));
                }
            },
        }
    }

    Ok(facts)
}

fn parse_fact_vector(lexer: &mut EdnLexer<'_>) -> Result<Fact> {
    let mut tokens = Vec::with_capacity(6);

    loop {
        lexer.skip_whitespace_and_commas();
        match lexer.peek_char() {
            Some(']') => {
                lexer.next_char();
                break;
            }
            None => {
                return Err(MesoError::Serialization(
                    "Unterminated inner fact clause".into(),
                ));
            }
            _ => {
                if let Some(tok) = lexer.next_token()? {
                    tokens.push(tok);
                }
            }
        }
    }

    if tokens.len() < 4 {
        return Err(MesoError::Serialization(
            "Fact clauses require at least 4 arguments: [op e a v]".into(),
        ));
    }

    let op = match &tokens[0] {
        RawToken::Keyword(k) if *k == ":db/add" => true,
        RawToken::Keyword(k) if *k == ":db/retract" => false,
        _ => {
            return Err(MesoError::Serialization(
                "First argument in a fact clause must be either :db/add or :db/retract".into(),
            ));
        }
    };

    let e = match &tokens[1] {
        RawToken::Int(i) => *i as u64,
        _ => {
            return Err(MesoError::Serialization(
                "Entity ID must resolve to an unsigned integer".into(),
            ));
        }
    };

    let ident = match &tokens[2] {
        RawToken::Keyword(k) => k.to_string(),
        _ => {
            return Err(MesoError::Serialization(
                "Fact attribute field must be a valid keyword string".into(),
            ));
        }
    };

    let v = match &tokens[3] {
        RawToken::String(s) => Value::String(s.to_string()),
        RawToken::Keyword(k) => Value::String(k.to_string()), // Keyword coercion matching old engine constraints
        RawToken::Int(i) => Value::Int64(*i),
        RawToken::Float(f) => Value::Float64(*f),
        RawToken::Inst(t) => Value::Timestamp(*t),
        _ => {
            return Err(MesoError::Serialization(
                "Unsupported structural primitive inside value cell".into(),
            ));
        }
    };

    let mut valid_time = None;
    if tokens.len() >= 5 {
        match &tokens[4] {
            RawToken::Inst(t) => valid_time = Some(*t),
            _ => {
                return Err(MesoError::Serialization(
                    "Valid-time parameter (5th cell index) must be an #inst literal timestamp"
                        .into(),
                ));
            }
        }
    }

    Ok(Fact {
        e,
        ident,
        v,
        op,
        cas_old_v: None,
        valid_time,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lexer_token_boundaries_zero_allocation() {
        let raw_input = r#"[ :db/add 42 :user/name "Delta Force" #inst "2026-07-17T23:00:00Z" ]"#;
        let mut lexer = EdnLexer::new(raw_input);

        assert_eq!(lexer.next_token().unwrap(), Some(RawToken::LeftBracket));
        assert_eq!(
            lexer.next_token().unwrap(),
            Some(RawToken::Keyword(":db/add"))
        );
        assert_eq!(lexer.next_token().unwrap(), Some(RawToken::Int(42)));
        assert_eq!(
            lexer.next_token().unwrap(),
            Some(RawToken::Keyword(":user/name"))
        );
        assert_eq!(
            lexer.next_token().unwrap(),
            Some(RawToken::String("Delta Force"))
        );

        let inst_tok = lexer.next_token().unwrap().unwrap();
        if let RawToken::Inst(ts) = inst_tok {
            assert!(ts > 0);
        } else {
            panic!("Expected structural tag token mapping to RawToken::Inst");
        }
        assert_eq!(lexer.next_token().unwrap(), Some(RawToken::RightBracket));
    }

    #[test]
    fn test_parse_transaction_matrix_compilation() {
        let input = r#"[
            [:db/add 100 :employee/salary 145000]
            [:db/retract 100 :employee/status "Probation" #inst "2026-01-01T00:00:00Z"]
        ]"#;

        let facts = parse_transaction(input).unwrap();
        assert_eq!(facts.len(), 2);

        // Core compilation validation - Fact 0
        assert!(facts[0].op);
        assert_eq!(facts[0].e, 100);
        assert_eq!(facts[0].ident, ":employee/salary");
        assert_eq!(facts[0].v, Value::Int64(145000));
        assert_eq!(facts[0].valid_time, None);

        // Core compilation validation - Fact 1
        assert!(!facts[1].op);
        assert_eq!(facts[1].e, 100);
        assert_eq!(facts[1].ident, ":employee/status");
        assert_eq!(facts[1].v, Value::String("Probation".to_string()));
        assert!(facts[1].valid_time.is_some());
    }

    #[test]
    fn test_malformed_syntax_traps() {
        assert!(
            parse_transaction("[:db/add 1 :a 2]").is_err(),
            "Must catch missing root bounding envelopes"
        );
        assert!(
            parse_transaction("[[:db/add 1]]").is_err(),
            "Must fail when transaction arguments do not meet element sizing rules"
        );
        assert!(
            parse_transaction(r#"[[:db/add 1 :a "missing_quote]]"#).is_err(),
            "Must correctly drop out on string unterminated errors"
        );
    }
}
