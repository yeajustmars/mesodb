// src/edn.rs

use chrono::DateTime;
use mesodb_core::transactor::Fact;
use mesodb_core::types::Value;

pub fn parse_edn_tx(input: &str) -> Result<Vec<Fact>, String> {
    let mut chars = input.chars().peekable();
    let mut facts = Vec::new();

    // Fast-forward to the opening bracket of the transaction vector
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() || c == '[' {
            if c == '[' {
                break;
            }
            chars.next();
        } else if input.starts_with("[:transact") {
            // Strip the REPL syntactic sugar if they included it
            for _ in 0..10 {
                chars.next();
            }
        } else {
            return Err("Transaction must start with '['".into());
        }
    }

    if chars.next() != Some('[') {
        return Err("Missing opening '[' for transaction".into());
    }

    while let Some(&c) = chars.peek() {
        match c {
            ']' => break, // End of transaction block
            '[' => {
                // Parse individual fact vector: [:db/add e a v]
                chars.next(); // Consume '['
                let fact = parse_fact_vector(&mut chars)?;
                facts.push(fact);
            }
            _ if c.is_whitespace() => {
                chars.next(); // Skip whitespace
            }
            _ => {
                return Err(format!(
                    "Unexpected character in transaction block: '{}'",
                    c
                ));
            }
        }
    }

    Ok(facts)
}

fn parse_fact_vector(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<Fact, String> {
    let mut tokens = Vec::new();

    while let Some(&c) = chars.peek() {
        match c {
            ']' => {
                chars.next(); // Consume ']'
                break;
            }
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
            }
            '"' => {
                // Parse String
                chars.next();
                let mut s = String::new();
                while let Some(sc) = chars.next() {
                    if sc == '"' {
                        break;
                    }
                    s.push(sc);
                }
                tokens.push(Token::String(s));
            }
            '#' => {
                // Parse #inst
                chars.next();
                let mut tag = String::new();
                while let Some(&tc) = chars.peek() {
                    if tc.is_whitespace() || tc == '"' {
                        break;
                    }
                    tag.push(chars.next().unwrap());
                }
                if tag == "inst" {
                    while let Some(&wc) = chars.peek() {
                        if !wc.is_whitespace() {
                            break;
                        }
                        chars.next();
                    }
                    if chars.next() != Some('"') {
                        return Err("Expected '\"' after #inst".into());
                    }
                    let mut s = String::new();
                    while let Some(sc) = chars.next() {
                        if sc == '"' {
                            break;
                        }
                        s.push(sc);
                    }
                    let dt = DateTime::parse_from_rfc3339(&s)
                        .map_err(|e| format!("Invalid #inst format: {}", e))?;
                    tokens.push(Token::Inst(dt.timestamp_micros()));
                } else {
                    return Err(format!("Unknown tag: #{}", tag));
                }
            }
            _ => {
                // Parse Keyword or Number
                let mut s = String::new();
                while let Some(&tc) = chars.peek() {
                    if tc.is_whitespace() || tc == ']' {
                        break;
                    }
                    s.push(chars.next().unwrap());
                }
                if s.starts_with(':') {
                    tokens.push(Token::Keyword(s));
                } else if let Ok(i) = s.parse::<i64>() {
                    tokens.push(Token::Int(i));
                } else if let Ok(f) = s.parse::<f64>() {
                    tokens.push(Token::Float(f));
                } else {
                    tokens.push(Token::Symbol(s));
                }
            }
        }
    }

    if tokens.len() < 4 {
        return Err("Fact vector must have at least 4 elements: [op e a v]".into());
    }

    let op = match &tokens[0] {
        Token::Keyword(k) if k == ":db/add" => true,
        Token::Keyword(k) if k == ":db/retract" => false,
        _ => return Err("First element must be :db/add or :db/retract".into()),
    };

    let e = match &tokens[1] {
        Token::Int(i) => *i as u64,
        _ => return Err("Entity ID must be an integer".into()),
    };

    let ident = match &tokens[2] {
        Token::Keyword(k) => k.clone(),
        _ => return Err("Attribute must be a keyword".into()),
    };

    let v = match &tokens[3] {
        Token::String(s) => Value::String(s.clone()),
        Token::Int(i) => Value::Int64(*i),
        Token::Float(f) => Value::Float64(*f),
        Token::Keyword(k) => Value::String(k.clone()), // Coerce keywords to strings for now
        _ => return Err("Unsupported value type".into()),
    };

    let valid_time = if tokens.len() >= 5 {
        match &tokens[4] {
            Token::Inst(micros) => Some(*micros),
            _ => return Err("Valid time must be an #inst literal".into()),
        }
    } else {
        None
    };

    Ok(Fact {
        e,
        ident,
        v,
        op,
        cas_old_v: None,
        valid_time,
    })
}

#[derive(Debug)]
enum Token {
    Keyword(String),
    String(String),
    Int(i64),
    Float(f64),
    Inst(i64),
    Symbol(String),
}
