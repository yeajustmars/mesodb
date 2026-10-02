// mesodb-cli/src/commands/mod.rs

use crate::engine::{MesoEngine, QueryOptions};
use clap::Subcommand;

#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum MesoCommand {
    /// Execute a zero-copy Datalog query
    Query {
        #[arg(help = "The EDN Datalog query string")]
        query: String,

        #[arg(
            short,
            long,
            default_value = "table",
            help = "Output format (table, edn, json)"
        )]
        format: String,

        #[arg(short, long, help = "Time-travel to a specific microsecond timestamp")]
        as_of: Option<i64>,
    },

    /// Submit an atomic EDN transaction
    Transact {
        #[arg(help = "The EDN transaction fact vector")]
        edn: String,
    },

    /// View engine memory, WAL, and compaction status
    Status,

    /// Trigger background Parquet compaction
    Compact,
}

/// Executes a parsed CLI command against any implementation of MesoEngine
pub async fn execute_command(engine: &dyn MesoEngine, cmd: MesoCommand) -> Result<(), String> {
    match cmd {
        MesoCommand::Query {
            query,
            format,
            as_of,
        } => {
            // "table" is a CLI presentation layer; the underlying transport remains EDN.
            let engine_format = if format == "table" {
                "edn".to_string()
            } else {
                format.clone()
            };
            let opts = QueryOptions {
                format: engine_format,
                as_of,
            };

            match engine.query(&query, opts).await {
                Ok(res) => {
                    if format == "table" {
                        match crate::format::render_table(&res.raw_output) {
                            Ok(table_str) => println!("{}", table_str),
                            Err(e) => eprintln!(
                                "Failed to render table: {}\nRaw output:\n{}",
                                e, res.raw_output
                            ),
                        }
                    } else {
                        // Raw EDN or JSON pass-through
                        println!("{}", res.raw_output);
                    }
                    Ok(())
                }
                Err(e) => Err(e.to_string()),
            }
        }
        MesoCommand::Transact { edn } => match engine.transact(&edn).await {
            Ok(report) => {
                println!(
                    "✅ Transaction {} committed. (Datoms written: {})",
                    report.tx_id, report.datoms_written
                );
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        },
        MesoCommand::Status => match engine.server_status().await {
            Ok(report) => {
                println!("📊 Status:\n{}", report.status);
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        },
        MesoCommand::Compact => match engine.trigger_compaction().await {
            Ok(report) => {
                println!(
                    "🗜️ Compaction triggered. Bytes freed: {}",
                    report.bytes_freed
                );
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meso_command_query_parsing() {
        let cmd = MesoCommand::Query {
            query: "[:find ?e]".to_string(),
            format: "table".to_string(),
            as_of: None,
        };

        if let MesoCommand::Query { format, as_of, .. } = cmd {
            assert_eq!(format, "table");
            assert_eq!(as_of, None);
        } else {
            panic!("Variant mismatch");
        }
    }
}
