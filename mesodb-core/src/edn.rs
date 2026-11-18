use chrono::DateTime;
use std::borrow::Cow;
use std::iter::Peekable;
use std::str::Chars;

use crate::error::MesoError;
use crate::transactor::Fact;
use crate::types::{Result, Value};

#[derive(Debug, PartialEq)]
enum RawToken<'a> {
    Keyword(&'a str),
    String(Cow<'a, str>),
    Int(i64),
    Float(f64),
    Boolean(bool),
    Nil,
    Inst(i64),
    Uuid([u8; 16]),
    Symbol(&'a str),
    LeftBracket,
    RightBracket,
    LeftBrace,
    RightBrace,
    HashBrace,
}

fn is_delimiter(c: char) -> bool {
    c.is_whitespace() || matches!(c, ',' | ';' | '[' | ']' | '{' | '}' | '(' | ')' | '"')
}

fn unescape_edn_str(raw: &str) -> Result<Cow<'_, str>> {
    if !raw.contains('\\') {
        return Ok(Cow::Borrowed(raw));
    }
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('"') => out.push('"'),
                Some('\\') => out.push('\\'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('t') => out.push('\t'),
                Some(other) => {
                    return Err(MesoError::Serialization(format!(
                        "Unsupported escape sequence '\\{other}' in EDN string"
                    )));
                }
                None => {
                    return Err(MesoError::Serialization(
                        "Unterminated escape sequence in EDN string".into(),
                    ));
                }
            }
        } else {
            out.push(c);
        }
    }
    Ok(Cow::Owned(out))
}

fn parse_uuid_bytes(s: &str) -> Result<[u8; 16]> {
    let bytes = s.as_bytes();
    if bytes.len() != 36
        || bytes[8] != b'-'
        || bytes[13] != b'-'
        || bytes[18] != b'-'
        || bytes[23] != b'-'
    {
        return Err(MesoError::Serialization(format!(
            "Invalid #uuid format: {s}"
        )));
    }
    let mut out = [0u8; 16];
    let mut out_idx = 0;
    let mut i = 0;
    while i < 36 {
        if bytes[i] == b'-' {
            i += 1;
            continue;
        }
        let hex_pair = &s[i..i + 2];
        out[out_idx] = u8::from_str_radix(hex_pair, 16)
            .map_err(|_| MesoError::Serialization(format!("Invalid #uuid hex: {s}")))?;
        out_idx += 1;
        i += 2;
    }
    Ok(out)
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
            } else if c == ';' {
                while let Some(ch) = self.next_char() {
                    if ch == '\n' {
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }

    fn read_quoted_raw(&mut self, unterminated_msg: &str) -> Result<&'a str> {
        self.next_char(); // Consume opening quote
        let start = self.byte_idx;
        let mut escaping = false;
        while let Some(c) = self.next_char() {
            if escaping {
                escaping = false;
            } else if c == '\\' {
                escaping = true;
            } else if c == '"' {
                let end = self.byte_idx - 1;
                return Ok(&self.input[start..end]);
            }
        }
        Err(MesoError::Serialization(unterminated_msg.into()))
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
            '{' => {
                self.next_char();
                Ok(Some(RawToken::LeftBrace))
            }
            '}' => {
                self.next_char();
                Ok(Some(RawToken::RightBrace))
            }
            '"' => {
                let raw = self.read_quoted_raw("Unterminated string literal")?;
                let unescaped = unescape_edn_str(raw)?;
                Ok(Some(RawToken::String(unescaped)))
            }
            '#' => {
                self.next_char(); // Consume '#'
                if self.peek_char() == Some(&'{') {
                    self.next_char();
                    return Ok(Some(RawToken::HashBrace));
                }

                let mut tag = String::new();
                while let Some(&c) = self.peek_char() {
                    if is_delimiter(c) {
                        break;
                    }
                    tag.push(self.next_char().unwrap());
                }

                match tag.as_str() {
                    "inst" => {
                        self.skip_whitespace_and_commas();
                        if self.peek_char() != Some(&'"') {
                            return Err(MesoError::Serialization(
                                "Expected string literal after #inst".into(),
                            ));
                        }
                        let dt_str = self.read_quoted_raw("Unterminated #inst literal")?;
                        let dt = DateTime::parse_from_rfc3339(dt_str).map_err(|e| {
                            MesoError::Serialization(format!("Invalid #inst format: {e}"))
                        })?;
                        Ok(Some(RawToken::Inst(dt.timestamp_micros())))
                    }
                    "uuid" => {
                        self.skip_whitespace_and_commas();
                        if self.peek_char() != Some(&'"') {
                            return Err(MesoError::Serialization(
                                "Expected string literal after #uuid".into(),
                            ));
                        }
                        let uuid_str = self.read_quoted_raw("Unterminated #uuid literal")?;
                        let bytes = parse_uuid_bytes(uuid_str)?;
                        Ok(Some(RawToken::Uuid(bytes)))
                    }
                    _ => Err(MesoError::Serialization(format!("Unknown tag: #{tag}"))),
                }
            }
            _ => {
                let start = self.byte_idx;
                while let Some(&c) = self.peek_char() {
                    if is_delimiter(c) {
                        break;
                    }
                    self.next_char();
                }
                let end = self.byte_idx;
                let slice = &self.input[start..end];

                if slice.is_empty() {
                    let unexpected = self.next_char().unwrap();
                    return Err(MesoError::Serialization(format!(
                        "Unexpected delimiter '{unexpected}' in EDN input"
                    )));
                }

                if slice.starts_with(':') {
                    Ok(Some(RawToken::Keyword(slice)))
                } else if slice == "true" {
                    Ok(Some(RawToken::Boolean(true)))
                } else if slice == "false" {
                    Ok(Some(RawToken::Boolean(false)))
                } else if slice == "nil" {
                    Ok(Some(RawToken::Nil))
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

fn token_to_value(tok: RawToken<'_>) -> Result<Value> {
    match tok {
        RawToken::String(s) => Ok(Value::String(s.into_owned())),
        RawToken::Keyword(k) => Ok(Value::String(k.to_string())),
        RawToken::Int(i) => Ok(Value::Int64(i)),
        RawToken::Float(f) => Ok(Value::Float64(f)),
        RawToken::Boolean(b) => Ok(Value::Boolean(b)),
        RawToken::Inst(t) => Ok(Value::Timestamp(t)),
        RawToken::Uuid(u) => Ok(Value::Uuid(u)),
        _ => Err(MesoError::Serialization(
            "Unsupported structural primitive inside value cell".into(),
        )),
    }
}

pub fn parse_transaction(input: &str) -> Result<Vec<Fact>> {
    let mut lexer = EdnLexer::new(input);

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
                lexer.next_char();
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
                Some(RawToken::LeftBrace) => {
                    parse_fact_map(&mut lexer, &mut facts)?;
                }
                _ => {
                    return Err(MesoError::Serialization(
                        "Expected inner fact vector '[' or entity map '{'".into(),
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

    let mut iter = tokens.into_iter();
    let op_tok = iter.next().unwrap();
    let e_tok = iter.next().unwrap();
    let a_tok = iter.next().unwrap();
    let v_tok = iter.next().unwrap();
    let vt_tok = iter.next();

    let op = match op_tok {
        RawToken::Keyword(":db/add") => true,
        RawToken::Keyword(":db/retract") => false,
        _ => {
            return Err(MesoError::Serialization(
                "First argument in a fact clause must be either :db/add or :db/retract".into(),
            ));
        }
    };

    let e = match e_tok {
        RawToken::Int(i) if i >= 0 => i as u64,
        _ => {
            return Err(MesoError::Serialization(
                "Entity ID must resolve to an unsigned integer".into(),
            ));
        }
    };

    let ident = match a_tok {
        RawToken::Keyword(k) => k.to_string(),
        _ => {
            return Err(MesoError::Serialization(
                "Fact attribute field must be a valid keyword string".into(),
            ));
        }
    };

    let v = token_to_value(v_tok)?;

    let mut valid_time = None;
    if let Some(vt) = vt_tok {
        match vt {
            RawToken::Inst(t) => valid_time = Some(t),
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

fn parse_fact_map(lexer: &mut EdnLexer<'_>, out_facts: &mut Vec<Fact>) -> Result<()> {
    let mut entity_id: Option<u64> = None;
    let mut attrs: Vec<(String, Vec<Value>)> = Vec::new();

    loop {
        lexer.skip_whitespace_and_commas();
        match lexer.peek_char() {
            Some('}') => {
                lexer.next_char();
                break;
            }
            None => {
                return Err(MesoError::Serialization(
                    "Unterminated entity map in transaction".into(),
                ));
            }
            _ => {
                let key_tok = lexer.next_token()?.ok_or_else(|| {
                    MesoError::Serialization("Unexpected EOF in entity map".into())
                })?;
                let key = match key_tok {
                    RawToken::Keyword(k) => k,
                    _ => {
                        return Err(MesoError::Serialization(
                            "Entity map keys must be keywords".into(),
                        ));
                    }
                };

                let val_tok = lexer.next_token()?.ok_or_else(|| {
                    MesoError::Serialization("Missing value for key in entity map".into())
                })?;

                if key == ":db/id" {
                    match val_tok {
                        RawToken::Int(i) if i >= 0 => entity_id = Some(i as u64),
                        _ => {
                            return Err(MesoError::Serialization(
                                ":db/id in entity map must be an unsigned integer".into(),
                            ));
                        }
                    }
                } else if matches!(val_tok, RawToken::HashBrace) {
                    let mut set_vals = Vec::new();
                    loop {
                        lexer.skip_whitespace_and_commas();
                        match lexer.peek_char() {
                            Some('}') => {
                                lexer.next_char();
                                break;
                            }
                            None => {
                                return Err(MesoError::Serialization(
                                    "Unterminated set '#{' in entity map".into(),
                                ));
                            }
                            _ => {
                                let elem = lexer.next_token()?.unwrap();
                                set_vals.push(token_to_value(elem)?);
                            }
                        }
                    }
                    attrs.push((key.to_string(), set_vals));
                } else {
                    attrs.push((key.to_string(), vec![token_to_value(val_tok)?]));
                }
            }
        }
    }

    let e = entity_id.ok_or_else(|| {
        MesoError::Serialization("Entity map transaction requires a :db/id key".into())
    })?;

    for (ident, vals) in attrs {
        for v in vals {
            out_facts.push(Fact {
                e,
                ident: ident.clone(),
                v,
                op: true,
                cas_old_v: None,
                valid_time: None,
            });
        }
    }

    Ok(())
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
            Some(RawToken::String(Cow::Borrowed("Delta Force")))
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

    #[test]
    fn test_lexer_brutal_escape_sequences() {
        // String 1: Rust macro with embedded quotes -> "fn main() { println!(\"Hello\"); }"
        // String 2: Windows file path with trailing escaped backslash -> "C:\\Temp\\"
        // String 3: Consecutive escapes and quotes -> "\\\"\\\\\""
        let raw_input = r#"[
            [:db/add 100 :code/signature "fn main() { println!(\"Hello\"); }"]
            [:db/add 101 :doc/text "C:\\Temp\\"]
            [:db/add 102 :doc/text "\\\"\\\\\""]
        ]"#;

        let facts = parse_transaction(raw_input).expect("Failed to parse brutal escape sequences");
        assert_eq!(facts.len(), 3);

        if let Value::String(s) = &facts[0].v {
            assert_eq!(s, r#"fn main() { println!("Hello"); }"#);
        } else {
            panic!("Expected String value for Fact 0");
        }

        if let Value::String(s) = &facts[1].v {
            assert_eq!(s, r"C:\Temp\");
        } else {
            panic!("Expected String value for Fact 1");
        }

        if let Value::String(s) = &facts[2].v {
            assert_eq!(s, "\\\"\\\\\"");
        } else {
            panic!("Expected String value for Fact 2");
        }
    }

    #[test]
    fn test_edn_comments_uuid_and_map_transactions() {
        let input = r#"[
            ; Line comment before vector fact
            [:db/add 1 :user/id #uuid "550e8400-e29b-41d4-a716-446655440000"]
            ; Map assertion with multi-valued set
            {:db/id 2, :user/name "Alice\nSmith", :user/tag #{"rust" "datalog"}}
        ]"#;

        let facts = parse_transaction(input).unwrap();
        assert_eq!(facts.len(), 4);
        assert!(matches!(facts[0].v, Value::Uuid(_)));
        assert_eq!(facts[1].e, 2);
        assert_eq!(facts[1].v, Value::String("Alice\nSmith".into()));
        assert_eq!(facts[2].v, Value::String("rust".into()));
        assert_eq!(facts[3].v, Value::String("datalog".into()));
    }

    #[test]
    fn test_lexer_unterminated_escapes() {
        // The string ends abruptly with a backslash right before the quote.
        // The lexer must treat the quote as escaped and realize the string never actually terminates.
        let raw_input = r#"[ [:db/add 100 :doc/text "This quote is escaped \"] ]"#;

        let result = parse_transaction(raw_input);
        assert!(
            result.is_err(),
            "Must fail when the trailing quote is escaped and no valid closing quote exists"
        );

        if let Err(e) = result {
            assert!(
                e.to_string().contains("Unterminated string literal"),
                "Expected unterminated string error, got: {}",
                e
            );
        }
    }

    #[test]
    fn test_parse_transaction_with_booleans() {
        let input = r#"[
            [:db/add 200 :user/active true]
            [:db/add 201 :user/active false]
        ]"#;

        let facts = parse_transaction(input).expect("Failed to parse boolean transaction");
        assert_eq!(facts.len(), 2);

        // Validate true
        assert!(facts[0].op);
        assert_eq!(facts[0].e, 200);
        assert_eq!(facts[0].ident, ":user/active");
        assert_eq!(facts[0].v, Value::Boolean(true));

        // Validate false
        assert!(facts[1].op);
        assert_eq!(facts[1].e, 201);
        assert_eq!(facts[1].ident, ":user/active");
        assert_eq!(facts[1].v, Value::Boolean(false));
    }

    #[test]
    fn test_parse_empty_transaction() {
        // An empty EDN vector should safely parse into an empty batch, not throw a structural error.
        let input = "[]";
        let facts = parse_transaction(input).expect("Failed to parse empty transaction");
        assert!(facts.is_empty(), "Empty transaction should yield 0 facts");

        let input_with_spaces = "[   \n \t ]";
        let facts_spaces = parse_transaction(input_with_spaces).expect("Failed on whitespace-only");
        assert!(facts_spaces.is_empty());
    }

    #[test]
    fn test_edn_commas_as_whitespace() {
        // In EDN, commas are treated entirely as whitespace.
        // We must ensure the lexer strips them safely without breaking token alignment.
        let input = r#"[ , [ , :db/add , 100 , :system/config , true , ] , ]"#;

        let facts = parse_transaction(input).expect("Failed to parse transaction with commas");
        assert_eq!(facts.len(), 1);

        assert!(facts[0].op);
        assert_eq!(facts[0].e, 100);
        assert_eq!(facts[0].ident, ":system/config");
        assert_eq!(facts[0].v, Value::Boolean(true));
    }

    #[test]
    fn test_invalid_entity_id_type() {
        // Entity IDs must strictly be unsigned integers.
        // Passing floats, negatives, or strings should aggressively trap.
        let input_float = r#"[ [:db/add 1.5 :user/name "Alice"] ]"#;
        let err_float = parse_transaction(input_float).unwrap_err();
        assert!(
            err_float
                .to_string()
                .contains("Entity ID must resolve to an unsigned integer")
        );

        let input_string = r#"[ [:db/add "100" :user/name "Alice"] ]"#;
        let err_string = parse_transaction(input_string).unwrap_err();
        assert!(
            err_string
                .to_string()
                .contains("Entity ID must resolve to an unsigned integer")
        );
    }

    #[test]
    fn test_invalid_operation_keyword() {
        // The engine exclusively supports :db/add and :db/retract.
        // Any other transaction keyword (e.g., :db/upsert) must fail immediately.
        let input = r#"[ [:db/upsert 100 :user/name "Alice"] ]"#;

        let err = parse_transaction(input).unwrap_err();
        assert!(
            err.to_string()
                .contains("First argument in a fact clause must be either :db/add or :db/retract")
        );
    }

    #[test]
    fn test_fact_argument_count_boundaries() {
        // Facts require exactly 4 or 5 arguments: [op e a v] or [op e a v valid_time]
        // Testing too few arguments (3).
        let input_short = r#"[ [:db/add 100 :user/name] ]"#;
        let err_short = parse_transaction(input_short).unwrap_err();
        assert!(
            err_short
                .to_string()
                .contains("Fact clauses require at least 4 arguments")
        );

        // Testing invalid 5th argument type (passing an int instead of an #inst literal)
        let input_bad_time = r#"[ [:db/add 100 :user/name "Alice" 123456] ]"#;
        let err_bad_time = parse_transaction(input_bad_time).unwrap_err();
        assert!(
            err_bad_time.to_string().contains(
                "Valid-time parameter (5th cell index) must be an #inst literal timestamp"
            )
        );
    }
}
