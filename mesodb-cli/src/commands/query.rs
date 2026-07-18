// mesodb-cli/src/commands/query.rs

//use comfy_table::{Cell, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};
use reqwest::Client;

pub async fn execute(
    client: &Client,
    base_url: &str,
    query: &str,
    as_of: Option<i64>, // We will upgrade this to accept #inst Strings in Step 4
) -> color_eyre::Result<()> {
    let url = format!("{}/query", base_url);

    // 1. Construct the native EDN map payload
    let as_of_edn = match as_of {
        Some(t) => format!(" :as-of {}", t),
        None => "".to_string(),
    };

    // Naive escape for internal quotes in the query string
    let escaped_query = query.replace('"', "\\\"");
    let payload = format!("{{:query \"{}\"{}}}", escaped_query, as_of_edn);

    // 2. Stream the EDN payload to the server
    let res = client
        .post(&url)
        .header("Content-Type", "application/edn")
        .body(payload)
        .send()
        .await?;

    let status = res.status();
    let body_text = res.text().await?;

    // 3. Handle response
    if status.is_success() {
        // TODO: We need an EDN parser here to rebuild the `comfy-table`!
        println!("✅ Query Result (Raw EDN):\n{}", body_text);
    } else {
        eprintln!("❌ Error ({}): {}", status, body_text);
    }

    Ok(())
}
