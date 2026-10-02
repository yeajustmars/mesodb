// mesodb-cli/src/main.rs

use clap::Parser;
use color_eyre::Result;
use reedline::{FileBackedHistory, Reedline, Signal};
use std::sync::Arc;

mod commands;
mod engine;
mod format;
mod repl;

use commands::MesoCommand;
use engine::{MesoEngine, embedded::EmbeddedEngine, remote::RemoteEngine};
use mesodb_core::{config::Config, db::MesoDB, schema::SchemaMap};

#[derive(Parser, Debug)]
#[command(
    name = "mesodb",
    version,
    about = "MesoDB Interactive CLI & Server Client"
)]
struct Cli {
    #[arg(
        long,
        env = "MESODB_ENDPOINT",
        help = "Connect to a remote MesoDB Server (e.g. http://127.0.0.1:8000)"
    )]
    endpoint: Option<String>,

    #[arg(long, help = "Path to run an embedded MesoDB instance directly")]
    db_path: Option<String>,

    #[command(subcommand)]
    command: Option<MesoCommand>,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();

    // 1. Resolve Engine Backend
    let mut _ephemeral_dir = None;

    let (engine, target_label): (Arc<dyn MesoEngine>, String) = if let Some(endpoint) = cli.endpoint
    {
        (Arc::new(RemoteEngine::new(endpoint.clone())), endpoint)
    } else if let Some(path) = cli.db_path {
        let core_db = MesoDB::open(&path, SchemaMap::new(), Config::default())
            .expect("Failed to boot embedded MesoDB engine");
        (
            Arc::new(EmbeddedEngine::new(Arc::new(core_db))),
            format!("local: {}", path),
        )
    } else {
        // DEFAULT UX: Boot an ephemeral DB in a temporary directory
        let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
        let default_path = temp_dir.path().join("ephemeral.db");
        let core_db = MesoDB::open(&default_path, SchemaMap::new(), Config::default())
            .expect("Failed to boot embedded MesoDB engine");

        // Keep the TempDir handle alive until the CLI process exits
        _ephemeral_dir = Some(temp_dir);

        (
            Arc::new(EmbeddedEngine::new(Arc::new(core_db))),
            "local: ephemeral".to_string(),
        )
    };

    // 2. Execute Subcommand (Non-Interactive)
    if let Some(cmd) = cli.command {
        if let Err(e) = commands::execute_command(engine.as_ref(), cmd).await {
            eprintln!("❌ Error: {}", e);
            std::process::exit(1);
        }
        return Ok(());
    }

    // 3. Launch Interactive REPL
    println!("MesoDB Interactive Shell");
    println!("Type .help for commands, or directly enter EDN to query/transact.\n");

    let history = Box::new(
        FileBackedHistory::with_file(1000, "mesodb_history.txt".into())
            .expect("Failed to setup history file"),
    );

    let mut line_editor = Reedline::create()
        .with_history(history)
        .with_validator(Box::new(repl::MesoValidator))
        .with_highlighter(Box::new(repl::MesoHighlighter))
        .with_completer(Box::new(repl::MesoCompleter::new()));

    let prompt = repl::MesoPrompt::new(target_label);

    loop {
        let sig = line_editor.read_line(&prompt);

        match sig {
            Ok(Signal::Success(buffer)) => {
                let input = buffer.trim();
                if input.is_empty() {
                    continue;
                }

                if let Err(e) = process_repl_input(engine.as_ref(), input).await {
                    eprintln!("❌ Error: {}", e);
                }
            }
            Ok(Signal::CtrlC) => {
                println!("^C");
            }
            Ok(Signal::CtrlD) => {
                println!("Graceful exit requested.");
                break;
            }
            Err(err) => {
                eprintln!("REPL Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}

/// Routes raw REPL string inputs into appropriate Engine trait calls
async fn process_repl_input(engine: &dyn MesoEngine, input: &str) -> Result<(), String> {
    // 1. Meta Commands
    if input.starts_with('.') {
        let mut parts = input.split_whitespace();
        let cmd = parts.next().unwrap_or("");
        let rest = parts.collect::<Vec<&str>>().join(" ");

        match cmd {
            ".exit" | ".quit" | ".q" => std::process::exit(0),
            ".help" => {
                println!("Available Meta Commands:");
                println!("  .query <EDN>   - Execute a Datalog query");
                println!("  .tx <EDN>      - Execute a transaction");
                println!("  .status        - View server status");
                println!("  .compact       - Trigger background compaction");
                println!("  .exit          - Close the session");
                return Ok(());
            }
            ".status" => return commands::execute_command(engine, MesoCommand::Status).await,
            ".compact" => return commands::execute_command(engine, MesoCommand::Compact).await,
            ".query" => {
                return commands::execute_command(
                    engine,
                    MesoCommand::Query {
                        query: rest,
                        format: "table".to_string(), // Request tabular rendering instead of raw EDN
                        as_of: None,
                    },
                )
                .await;
            }
            ".tx" => {
                return commands::execute_command(engine, MesoCommand::Transact { edn: rest })
                    .await;
            }
            _ => return Err(format!("Unknown meta-command: {}", cmd)),
        }
    }

    // 2. Automatic Datalog Routing (Heuristic)
    let stripped = input.replace(' ', "").replace('\n', "");
    if stripped.starts_with("[:find") {
        commands::execute_command(
            engine,
            MesoCommand::Query {
                query: input.to_string(),
                format: "table".to_string(), // Request tabular rendering
                as_of: None,
            },
        )
        .await
    } else if stripped.starts_with("[[:") {
        commands::execute_command(
            engine,
            MesoCommand::Transact {
                edn: input.to_string(),
            },
        )
        .await
    } else {
        Err("Syntax Error: Datalog input must begin with '[:find' or '[[:'.".to_string())
    }
}

#[cfg(test)]
mod tests {
    // Basic verification that the module compiles cleanly.
    #[test]
    fn test_main_module_compiles() {
        assert!(true);
    }
}
