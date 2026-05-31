// mesodb-cli/src/main.rs

use clap::{Parser, Subcommand};
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

    /// Subcommands for specific CLI actions
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Bootstraps the database with a sample social graph dataset
    TryMeso,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    let http_client = Client::new();

    // Subcommand execution (e.g., mesodb try-meso)
    if let Some(cmd) = &cli.command {
        match cmd {
            Commands::TryMeso => {
                execute_try_meso(&http_client, &cli.url).await?;
                return Ok(());
            }
        }
    }

    // One-shot execution (e.g., used in bash scripts or piping)
    if let Some(q) = cli.query {
        execute_query(&http_client, &cli.url, &q).await?;
        return Ok(());
    }

    // Interactive REPL Loop
    println!("MesoDB Interactive CLI");
    println!("Connected to {}", cli.url);
    println!("Commands:");
    println!("  query [:find ...]      - Execute a Datalog query");
    println!("  transact [{{...}}]       - Execute a JSON transaction payload");
    println!("  exit                   - Quit the CLI");

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

async fn execute_try_meso(client: &Client, base_url: &str) -> color_eyre::Result<()> {
    println!("🚀 Bootstrapping MesoDB Sandbox Environment...");

    // 1. Define the Schema
    let schema_payload = serde_json::json!({
        "attributes": [
            { "ident": ":user/id", "value_type": "String", "is_unique": true },
            { "ident": ":user/name", "value_type": "String", "is_unique": false },
            { "ident": ":user/age", "value_type": "Int64", "is_unique": false },
            { "ident": ":user/follows", "value_type": "Ref", "is_unique": false }
        ]
    });

    println!("   -> Pushing schema topology to {}/schema...", base_url);
    let schema_res = client
        .post(format!("{}/schema", base_url))
        .json(&schema_payload)
        .send()
        .await?;

    if schema_res.status().is_success() {
        println!("   ✅ Schema established.");
    } else {
        eprintln!(
            "   ❌ Server rejected schema: {}",
            schema_res.text().await.unwrap_or_default()
        );
        return Ok(());
    }

    // 2. Insert the Dataset
    let tx_payload = serde_json::json!({
        "facts": [
            // Alice
            { "e": 100, "ident": ":user/id", "v": "u1", "op": true },
            { "e": 100, "ident": ":user/name", "v": "Alice", "op": true },
            { "e": 100, "ident": ":user/age", "v": 28, "op": true },
            // Bob
            { "e": 200, "ident": ":user/id", "v": "u2", "op": true },
            { "e": 200, "ident": ":user/name", "v": "Bob", "op": true },
            { "e": 200, "ident": ":user/age", "v": 32, "op": true },
            // Charlie
            { "e": 300, "ident": ":user/id", "v": "u3", "op": true },
            { "e": 300, "ident": ":user/name", "v": "Charlie", "op": true },
            { "e": 300, "ident": ":user/age", "v": 25, "op": true },

            // The Graph: Alice follows Bob, Bob follows Charlie
            { "e": 100, "ident": ":user/follows", "v": 200, "op": true },
            { "e": 200, "ident": ":user/follows", "v": 300, "op": true }
        ]
    });

    println!(
        "   -> Injecting social graph dataset to {}/transact...",
        base_url
    );
    let tx_res = client
        .post(format!("{}/transact", base_url))
        .json(&tx_payload)
        .send()
        .await?;

    if tx_res.status().is_success() {
        println!("   ✅ Dataset injected successfully.");
        println!("\n🎉 Sandbox ready! Try running this in the REPL:");
        println!("   query [:find ?name ?age :where [?e :user/name ?name] [?e :user/age ?age]]");
    } else {
        eprintln!(
            "   ❌ Server rejected dataset: {}",
            tx_res.text().await.unwrap_or_default()
        );
    }

    Ok(())
}

async fn execute_query(client: &Client, base_url: &str, query: &str) -> color_eyre::Result<()> {
    let url = format!("{}/query", base_url);

    // We specifically request the "json" format so we can parse it into the ASCII table
    let payload = serde_json::json!({
        "query": query,
        "options": {
            "format": "Json"
        }
    });

    let res = client.post(&url).json(&payload).send().await?;
    let status = res.status();
    let body_text = res.text().await?;

    if status.is_success() {
        // Handle wrapped {"status": "success", "results": [...]} or raw array responses
        let parsed_json: Value = serde_json::from_str(&body_text).unwrap_or(Value::Null);
        let rows_value = if let Some(results) = parsed_json.get("results") {
            // Parse embedded JSON string array
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

            // Extract dynamic headers from the keys of the first JSON object
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
