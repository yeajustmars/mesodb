// mesodb-cli/src/main.rs

use clap::{Parser, Subcommand};
use reedline::{FileBackedHistory, Reedline, Signal};
use reqwest::Client;

use mesodb_doc::print_doc_to_terminal;

mod commands;
mod edn;
mod repl;
mod tutor;

#[derive(Parser, Debug)]
#[command(name = "mesodb", version, about = "MesoDB Interactive Thin Client")]
struct Cli {
    #[arg(short, long)]
    uri: Option<String>,

    #[arg(short, long)]
    query: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    TryMeso,
}

pub enum SessionState {
    Disconnected,
    Connected { uri: String, client: Client },
    Tutor(Box<tutor::TutorSession>),
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();

    let mut session = if let Some(uri) = cli.uri {
        SessionState::Connected {
            uri,
            client: Client::new(),
        }
    } else {
        SessionState::Disconnected
    };

    if let Some(cmd) = &cli.command {
        match cmd {
            Commands::TryMeso => {
                println!("TryMeso execution moved to sandbox handler.");
                return Ok(());
            }
        }
    }

    if let Some(q) = cli.query {
        match &session {
            SessionState::Connected { uri, client } => {
                let _ = commands::query::execute(client, uri, &q, None).await;
            }
            _ => eprintln!("Error: Cannot execute shell query without a connection URI."),
        }
        return Ok(());
    }

    println!("MesoDB Interactive CLI");
    println!("Type .doc for help, .connect <uri> to connect, or .learn for the tutorial.\n");

    // Reedline Engine Initialization
    let history = Box::new(
        FileBackedHistory::with_file(1000, "mesodb_history.txt".into())
            .expect("Failed to setup history"),
    );
    let validator = Box::new(repl::MesoValidator);

    let mut line_editor = Reedline::create()
        .with_history(history)
        .with_validator(validator);

    loop {
        // Dynamic prompt evaluation
        let prompt_str = match &session {
            SessionState::Disconnected => "meso(offline)".to_string(),
            SessionState::Connected { .. } => "meso(active)".to_string(),
            SessionState::Tutor(_) => "meso(tutor)".to_string(), // Update this line!
        };

        let prompt = repl::MesoPrompt::new(prompt_str);

        // Reedline captures signals beautifully
        let sig = line_editor.read_line(&prompt);

        match sig {
            Ok(Signal::Success(buffer)) => {
                let input = buffer.trim();
                if input.is_empty() {
                    continue;
                }

                // 1. Global Escapes (Always available)
                let cmd = input.split_whitespace().next().unwrap_or("");
                if cmd == ".exit" || cmd == ".quit" || cmd == ".q" {
                    break;
                }

                // 2. State Interceptors (Tutor gets first dibs)
                if let SessionState::Tutor(ref mut tutor_session) = session {
                    let is_active = tutor_session.process_input(input).await; // <-- Added .await
                    if !is_active {
                        session = SessionState::Disconnected;
                    }
                    continue;
                }

                // 3. Normal Meta-Commands (Shell Control)
                if input.starts_with('.') {
                    match cmd {
                        ".connect" => commands::config::connect(&mut session, input),
                        ".doc" => {
                            // TODO: handle unicode strings (even though they're not valid here)
                            let name = input.get(5..).expect("Failed to get doc substring");
                            print_doc_to_terminal(name)?
                        }
                        ".help" => println!("TODO: impl help"),
                        ".learn" => {
                            let session_machine =
                                tutor::TutorSession::new(tutor::hr_story::build()).await;
                            session_machine.start();
                            session = SessionState::Tutor(Box::new(session_machine)); // Box it!
                        }
                        _ => eprintln!("Unknown shell command. Type .doc for help."),
                    }
                    continue;
                }

                // 4. Database Execution (Auto-Routing)
                if input.starts_with('[') {
                    // Datalog queries usually start with [:find
                    let is_query = input.replace(" ", "").starts_with("[:find");

                    match &session {
                        SessionState::Connected { uri, client } => {
                            if is_query {
                                let _ = commands::query::execute(client, uri, input, None).await;
                            } else {
                                let _ = commands::transact::execute(client, uri, input).await;
                            }
                        }
                        SessionState::Disconnected => {
                            eprintln!("⚠️ Not Connected. Use .connect :host <uri> or .learn.")
                        }
                        SessionState::Tutor(_) => unreachable!(), // Already handled by Block 2
                    }
                } else {
                    eprintln!(
                        "Syntax Error: Database commands must be valid EDN starting with '['."
                    );
                    eprintln!("Type .doc for help or .learn for an interactive tutorial.");
                }
            }
            Ok(Signal::CtrlC) => {
                println!("^C");
                continue;
            }
            Ok(Signal::CtrlD) => {
                println!("Graceful exit requested (EOF).");
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}
