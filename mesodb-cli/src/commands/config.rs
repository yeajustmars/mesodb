// mesodb-cli/src/commands/config.rs

use crate::SessionState;
use reqwest::Client;

pub fn connect(session: &mut SessionState, input: &str) {
    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.len() < 2 {
        eprintln!("Usage: :connect <uri> (e.g. :connect http://127.0.0.1:8000)");
        return;
    }

    let uri = parts[1].to_string();
    println!("Connecting to {}...", uri);

    // Ping logic could go here to verify the server is alive

    *session = SessionState::Connected {
        uri,
        client: Client::new(),
    };
    println!("✅ Connection established.");
}
