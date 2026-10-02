// mesodb-cli/src/format.rs

use comfy_table::{Cell, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};
use std::collections::HashMap;
use std::iter::Peekable;
use std::str::Chars;

/// Parses an EDN array of maps (MesoDB's query output format) and renders it as an ASCII table.
pub fn render_table(edn_str: &str) -> Result<String, String> {
    let trimmed = edn_str.trim();
    if trimmed == "[]" {
        return Ok("(0 rows returned)".to_string());
    }

    let rows = parse_edn_maps(trimmed)?;

    if rows.is_empty() {
        return Ok("(0 rows returned)".to_string());
    }

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS);

    // Extract headers from the keys of the first row
    let mut headers: Vec<String> = rows[0].keys().cloned().collect();
    headers.sort(); // Sort to keep column order deterministic
    table.set_header(&headers);

    // Populate rows
    for row_map in rows {
        let mut row_cells = Vec::new();
        for h in &headers {
            let cell_val = row_map.get(h).cloned().unwrap_or_else(|| "nil".to_string());
            row_cells.push(Cell::new(cell_val));
        }
        table.add_row(row_cells);
    }

    Ok(table.to_string())
}

// -----------------------------------------------------------------------------
// Lightweight EDN Query Result Parser
// -----------------------------------------------------------------------------

fn parse_edn_maps(input: &str) -> Result<Vec<HashMap<String, String>>, String> {
    let mut chars = input.chars().peekable();
    let mut rows = Vec::new();

    skip_whitespace(&mut chars);
    if chars.next() != Some('[') {
        return Err("Expected '[' at start of EDN query result".to_string());
    }

    loop {
        skip_whitespace(&mut chars);
        match chars.peek() {
            Some(&']') => {
                chars.next();
                break;
            }
            Some(&'{') => {
                chars.next();
                let map = parse_edn_map_body(&mut chars)?;
                rows.push(map);
            }
            _ => return Err("Expected '{' or ']' in EDN result array".to_string()),
        }
    }

    Ok(rows)
}

fn skip_whitespace(chars: &mut Peekable<Chars>) {
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() || c == ',' {
            chars.next();
        } else {
            break;
        }
    }
}

fn parse_edn_map_body(chars: &mut Peekable<Chars>) -> Result<HashMap<String, String>, String> {
    let mut map = HashMap::new();

    loop {
        skip_whitespace(chars);
        match chars.peek() {
            Some(&'}') => {
                chars.next();
                break;
            }
            Some(&':') => {
                let mut kw = String::new();
                chars.next(); // consume ':'
                kw.push(':');
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() || c == '}' {
                        break;
                    }
                    kw.push(chars.next().unwrap());
                }

                skip_whitespace(chars);
                let val = parse_edn_value(chars)?;
                map.insert(kw, val);
            }
            _ => return Err("Expected keyword or '}' in EDN map".to_string()),
        }
    }

    Ok(map)
}

fn parse_edn_value(chars: &mut Peekable<Chars>) -> Result<String, String> {
    match chars.peek() {
        Some(&'"') => {
            let mut s = String::new();
            chars.next(); // consume opening quote
            let mut escape = false;
            while let Some(c) = chars.next() {
                if escape {
                    s.push(c);
                    escape = false;
                } else if c == '\\' {
                    escape = true;
                } else if c == '"' {
                    break;
                } else {
                    s.push(c);
                }
            }
            // Return unquoted string for clean table display
            Ok(s)
        }
        Some(&'{') | Some(&'[') => {
            // Nested collection (e.g., from `pull`), capture raw string by balancing brackets
            let mut s = String::new();
            let start_char = chars.next().unwrap();
            s.push(start_char);

            let end_char = if start_char == '{' { '}' } else { ']' };
            let mut depth = 1;
            let mut in_string = false;
            let mut escape = false;

            while let Some(c) = chars.next() {
                s.push(c);
                if escape {
                    escape = false;
                } else if c == '\\' {
                    escape = true;
                } else if c == '"' {
                    in_string = !in_string;
                } else if !in_string {
                    if c == start_char {
                        depth += 1;
                    } else if c == end_char {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                }
            }
            Ok(s)
        }
        Some(_) => {
            // Literals (numbers, booleans, nil)
            let mut s = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_whitespace() || c == '}' || c == ']' {
                    break;
                }
                s.push(chars.next().unwrap());
            }
            Ok(s)
        }
        None => Err("Unexpected EOF while parsing EDN value".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_table_empty_array() {
        let edn = "[]";
        let res = render_table(edn).unwrap();
        assert_eq!(res, "(0 rows returned)");
    }

    #[test]
    fn test_render_table_populated_array() {
        let edn = r#"[ {:name "Alice" :age 30} {:name "Bob" :age 42} ]"#;
        let res = render_table(edn).unwrap();

        assert!(res.contains(":name"));
        assert!(res.contains(":age"));
        assert!(res.contains("Alice")); // Note: Quotes cleanly stripped
        assert!(res.contains("42"));
    }

    #[test]
    fn test_render_table_nested_pull_data() {
        // Nested EDN shouldn't break the parser
        let edn = r#"[ {:e 1 :user/address {:address/city "New York"}} ]"#;
        let res = render_table(edn).unwrap();

        assert!(res.contains(":user/address"));
        assert!(res.contains(r#"{:address/city "New York"}"#)); // Preserves inner structure
    }

    #[test]
    fn test_render_table_invalid_edn() {
        let res = render_table("NOT_EDN");
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("Expected '['"));
    }
}
