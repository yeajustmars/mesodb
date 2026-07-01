// src/commands/query.rs

use comfy_table::{Cell, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};
use reqwest::Client;
use serde_json::Value;

pub async fn execute(
    client: &Client,
    base_url: &str,
    query: &str,
    as_of: Option<i64>,
) -> color_eyre::Result<()> {
    let url = format!("{}/query", base_url);
    let payload = serde_json::json!({
        "query": query,
        "as_of": as_of,
        "format": "Json"
    });

    let res = client.post(&url).json(&payload).send().await?;
    let status = res.status();
    let body_text = res.text().await?;

    if status.is_success() {
        let parsed_json: Value = serde_json::from_str(&body_text).unwrap_or(Value::Null);
        let rows_value = if let Some(results) = parsed_json.get("results") {
            if let Some(s) = results.as_str() {
                serde_json::from_str(s).unwrap_or(Value::Array(vec![]))
            } else {
                results.clone()
            }
        } else {
            parsed_json
        };

        if let Some(rows) = rows_value.as_array() {
            if rows.is_empty() {
                println!("(0 rows returned)");
                return Ok(());
            }
            let first_obj = rows[0].as_object().unwrap();
            let headers: Vec<String> = first_obj.keys().cloned().collect();

            let mut table = Table::new();
            table
                .load_preset(UTF8_FULL)
                .apply_modifier(UTF8_ROUND_CORNERS)
                .set_header(&headers);

            for row_val in rows {
                if let Some(row) = row_val.as_object() {
                    let mut table_row = Vec::new();
                    for h in &headers {
                        let val_str = match row.get(h) {
                            Some(Value::String(s)) => s.to_string(),
                            Some(other) => other.to_string(),
                            None => "null".to_string(),
                        };
                        table_row.push(Cell::new(val_str));
                    }
                    table.add_row(table_row);
                }
            }
            println!("{table}");
        } else {
            println!("{}", body_text);
        }
    } else {
        eprintln!("Error ({}): {}", status, body_text);
    }
    Ok(())
}
