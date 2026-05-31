// mesodb-cli/src/main.rs

use clap::{Parser, Subcommand};
use comfy_table::{Cell, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};
use reqwest::Client;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(name = "mesodb", version, about = "MesoDB Interactive Thin Client")]
struct Cli {
    #[arg(short, long, default_value = "http://127.0.0.1:8080")]
    url: String,

    #[arg(short, long)]
    query: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Bootstraps the database with a bitemporal social graph dataset
    TryMeso,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    let http_client = Client::new();

    if let Some(cmd) = &cli.command {
        match cmd {
            Commands::TryMeso => {
                execute_try_meso(&http_client, &cli.url).await?;
                return Ok(());
            }
        }
    }

    if let Some(q) = cli.query {
        execute_query(&http_client, &cli.url, &q, None).await?;
        return Ok(());
    }

    // --- REPL STATE ---
    let mut current_as_of: Option<i64> = None;

    println!("MesoDB Interactive CLI");
    println!("Connected to {}", cli.url);
    println!("Commands:");
    println!("  query [:find ...]      - Execute a Datalog query");
    println!("  transact [{{...}}]       - Execute a JSON transaction payload");
    println!("  time <unix_ms>         - Set the time-travel context (e.g. time 1716082284000)");
    println!("  time now               - Reset time-travel context to the present");
    println!("  exit                   - Quit the CLI");

    let mut rl = DefaultEditor::new()?;
    let history_file = "mesodb_history.txt";
    let _ = rl.load_history(history_file);

    loop {
        // Dynamic prompt showing temporal state
        let prompt = match current_as_of {
            Some(t) => format!("mesodb [@{}]> ", t),
            None => "mesodb> ".to_string(),
        };

        let readline = rl.readline(&prompt);
        match readline {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(input);

                if input == "exit" || input == "quit" {
                    break;
                } else if let Some(t) = input.strip_prefix("time ") {
                    if t == "now" {
                        current_as_of = None;
                        println!("⏰ Time machine reset to present (now).");
                    } else if let Ok(ts) = t.parse::<i64>() {
                        current_as_of = Some(ts);
                        println!("⏰ Time machine set to: {}", ts);
                    } else {
                        eprintln!("Invalid time. Use 'time <unix_ms>' or 'time now'.");
                    }
                } else if let Some(q) = input.strip_prefix("query ") {
                    let _ = execute_query(&http_client, &cli.url, q, current_as_of).await;
                } else if let Some(t) = input.strip_prefix("transact ") {
                    let _ = execute_transact(&http_client, &cli.url, t).await;
                } else {
                    eprintln!(
                        "Unknown command. Prefix with 'query ', 'transact ', 'time ', or 'exit'."
                    );
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    let _ = rl.save_history(history_file);
    Ok(())
}

async fn execute_try_meso(client: &Client, base_url: &str) -> color_eyre::Result<()> {
    println!("🚀 Bootstrapping MesoDB Sandbox Environment...");

    let schema_payload = serde_json::json!({
        "attributes": [
            { "ident": ":user/id", "value_type": "String", "is_unique": true },
            { "ident": ":user/name", "value_type": "String", "is_unique": false },
            { "ident": ":user/age", "value_type": "Int64", "is_unique": false },
            { "ident": ":user/follows", "value_type": "Ref", "is_unique": false }
        ]
    });

    println!("   -> Pushing schema topology...");
    let _ = client
        .post(format!("{}/schema", base_url))
        .json(&schema_payload)
        .send()
        .await?;

    let t1 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    let tx1_payload = serde_json::json!({
        "facts": [
            { "e": 100, "ident": ":user/id", "v": "u1", "op": true },
            { "e": 100, "ident": ":user/name", "v": "Alice", "op": true },
            { "e": 100, "ident": ":user/age", "v": 28, "op": true },
            { "e": 200, "ident": ":user/id", "v": "u2", "op": true },
            { "e": 200, "ident": ":user/name", "v": "Bob", "op": true },
            { "e": 200, "ident": ":user/age", "v": 32, "op": true },
        ]
    });

    println!("   -> Injecting initial state...");
    let _ = client
        .post(format!("{}/transact", base_url))
        .json(&tx1_payload)
        .send()
        .await?;

    println!("   ⏳ Waiting 1 second to simulate time passing...");
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let tx2_payload = serde_json::json!({
        "facts": [
            // Alice has a birthday! Retraction is handled automatically by the bitemporal engine.
            { "e": 100, "ident": ":user/age", "v": 29, "op": true },
            // Alice follows Bob
            { "e": 100, "ident": ":user/follows", "v": 200, "op": true },
        ]
    });

    println!("   -> Injecting mutations (Alice's birthday!)...");
    let _ = client
        .post(format!("{}/transact", base_url))
        .json(&tx2_payload)
        .send()
        .await?;

    println!("   ✅ Sandbox ready!");
    println!("\n==========================================");
    println!("🧪 TIME TRAVEL TUTORIAL");
    println!("==========================================");
    println!("1. Run a normal query to see the present (Alice is 29):");
    println!("   query [:find ?name ?age :where [?e :user/name ?name] [?e :user/age ?age]]");
    println!("\n2. Set the time machine to right BEFORE her birthday:");
    println!("   time {}", t1 + 500_000); // Add 500ms so we are safely after T1 but before T2
    println!("\n3. Run the exact same query again. You'll see Alice is 28!");
    println!("\n4. Reset to the present:");
    println!("   time now");

    Ok(())
}

async fn execute_query(
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

async fn execute_transact(
    client: &Client,
    base_url: &str,
    facts_json: &str,
) -> color_eyre::Result<()> {
    let url = format!("{}/transact", base_url);
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
