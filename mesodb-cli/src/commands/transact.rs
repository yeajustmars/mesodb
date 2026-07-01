// src/commands/transact.rs

use reqwest::Client;
use serde_json::Value;

pub async fn execute(client: &Client, base_url: &str, facts_json: &str) -> color_eyre::Result<()> {
    let url = format!("{}/transact", base_url);

    // Note: Temporary JSON parser. Will be replaced by EDN parser later.
    let parsed_facts: Value = match serde_json::from_str(facts_json) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Invalid JSON syntax: {}", e);
            return Ok(());
        }
    };

    let payload = serde_json::json!({ "facts": parsed_facts });
    let res = client.post(&url).json(&payload).send().await?;
    let status = res.status();
    let body_text = res.text().await?;

    if status.is_success() {
        println!("Success: {}", body_text);
    } else {
        eprintln!("Error ({}): {}", status, body_text);
    }
    Ok(())
}
