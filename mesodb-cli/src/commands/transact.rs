// mesodb-cli/src/commands/transact.rs

use reqwest::Client;

use crate::edn;

pub async fn execute(client: &Client, base_url: &str, input: &str) -> color_eyre::Result<()> {
    let url = format!("{}/transact", base_url);

    // 1. Validate syntax locally to fail fast before hitting the network
    if let Err(e) = edn::parse_edn_tx(input) {
        eprintln!("Syntax Error: {}", e);
        return Ok(());
    }

    // 2. Stream the native EDN string directly to the server
    let res = client
        .post(&url)
        .header("Content-Type", "application/edn")
        .body(input.to_string())
        .send()
        .await?;

    let status = res.status();
    let body_text = res.text().await?;

    // 3. Print the server's EDN TxReport acknowledgement
    if status.is_success() {
        println!("✅ Transacted: {}", body_text);
    } else {
        eprintln!("❌ Error ({}): {}", status, body_text);
    }

    Ok(())
}
