// mesodb-cli/src/main.rs

use clap::Parser;
use comfy_table::{Cell, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};
use reqwest::Client;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use serde_json::Value;

#[derive(Parser, Debug)]
#[command(name = "mesodb", version, about = "MesoDB Interactive Thin Client")]
struct Cli {
    /// The HTTP URL of the MesoDB server
    #[arg(short, long, default_value = "http://127.0.0.1:8080")]
    url: String,

    /// Execute a single query and exit (One-shot mode)
    #[arg(short, long)]
    query: Option<String>,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    let http_client = Client::new();

    // One-shot execution (e.g., used in bash scripts or piping)
    if let Some(q) = cli.query {
        execute_query(&http_client, &cli.url, &q).await?;
        return Ok(());
    }

    // Interactive REPL Loop
    println!("MesoDB Interactive CLI");
    println!("Connected to {}", cli.url);
    println!("Commands:");
    println!("  query [:find ...]     - Execute a Datalog query");
    println!("  transact [{{...}}]      - Execute a JSON transaction payload");
    println!("  exit                  - Quit the CLI");

    // Initialize rustyline
    let mut rl = DefaultEditor::new()?;
    let history_file = "mesodb_history.txt";
    let _ = rl.load_history(history_file);

    loop {
        let readline = rl.readline("mesodb> ");
        match readline {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }

                // Add to rustyline history
                let _ = rl.add_history_entry(input);

                if input == "exit" || input == "quit" {
                    break;
                } else if let Some(q) = input.strip_prefix("query ") {
                    let _ = execute_query(&http_client, &cli.url, q).await;
                } else if let Some(t) = input.strip_prefix("transact ") {
                    let _ = execute_transact(&http_client, &cli.url, t).await;
                } else {
                    eprintln!("Unknown command. Prefix with 'query ', 'transact ', or 'exit'.");
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                // Handle Ctrl-C and Ctrl-D gracefully
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    // Save command history for the next session
    let _ = rl.save_history(history_file);
    Ok(())
}

async fn execute_query(client: &Client, base_url: &str, query: &str) -> color_eyre::Result<()> {
    let url = format!("{}/query", base_url);

    // We specifically request the "json" format so we can parse it into the ASCII table
    let payload = serde_json::json!({
        "query": query,
        "format": "json"
    });

    let res = client.post(&url).json(&payload).send().await?;
    let status = res.status();
    let body_text = res.text().await?;

    if status.is_success() {
        if let Ok(rows) = serde_json::from_str::<Vec<serde_json::Map<String, Value>>>(&body_text) {
            if rows.is_empty() {
                println!("(0 rows returned)");
                return Ok(());
            }

            // Extract dynamic headers from the keys of the first JSON object
            let headers: Vec<String> = rows[0].keys().cloned().collect();

            let mut table = Table::new();
            table
                .load_preset(UTF8_FULL)
                .apply_modifier(UTF8_ROUND_CORNERS)
                .set_header(&headers);

            for row in rows {
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
            println!("{table}");
        } else {
            // Fallback: if it's not a JSON array of objects, just print the raw response
            println!("{}", body_text);
        }
    } else {
        eprintln!("Error ({}): {}", status, body_text);
    }
    Ok(())
}

async fn execute_transact(
    client: &Client,
    base_url: &str,
    facts_json: &str,
) -> color_eyre::Result<()> {
    let url = format!("{}/transact", base_url);

    // Parse the raw string from the CLI to ensure it's valid JSON before sending
    let parsed_facts: Value = match serde_json::from_str(facts_json) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Invalid JSON syntax: {}", e);
            return Ok(());
        }
    };

    let payload = serde_json::json!({
        "facts": parsed_facts
    });

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
