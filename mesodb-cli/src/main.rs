use comfy_table::Table;
use mesodb_core::db::MesoDB;
use mesodb_core::schema::{SchemaMap, ValueType};
use mesodb_core::transactor::Fact;
use mesodb_core::types::Value;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🦀 MesoDB CLI v0.1.0");
    println!("Type 'exit' to quit.\n");

    // 1. Setup a basic schema for our demo
    let mut schema = SchemaMap::new();
    schema.add_attribute(":user/name", ValueType::String, false);
    schema.add_attribute(":user/email", ValueType::String, true);
    schema.add_attribute(":user/age", ValueType::Int64, false);

    // 2. Open the DB (using a temporary file for the REPL session)
    let temp_path = std::env::temp_dir().join("meso_cli.wal");
    let mut db = MesoDB::open(temp_path, schema)?;

    // Seed some initial data
    db.transact(vec![
        Fact {
            e: 1,
            ident: ":user/name".into(),
            v: Value::String("Alice".into()),
            op: true,
        },
        Fact {
            e: 1,
            ident: ":user/age".into(),
            v: Value::Int64(30),
            op: true,
        },
        Fact {
            e: 2,
            ident: ":user/name".into(),
            v: Value::String("Bob".into()),
            op: true,
        },
    ])?;

    let mut rl = DefaultEditor::new()?;

    loop {
        let readline = rl.readline("meso> ");
        match readline {
            Ok(line) => {
                let input = line.trim();
                if input == "exit" {
                    break;
                }
                if input.is_empty() {
                    continue;
                }

                // 3. Execute Query
                match db.query(input).await {
                    Ok(batches) => {
                        if batches.is_empty() || batches[0].num_rows() == 0 {
                            println!("Empty set.");
                            continue;
                        }

                        // 4. Pretty Print Results
                        let mut table = Table::new();
                        let schema = batches[0].schema();

                        // Set Headers from AST variable names
                        let headers: Vec<_> = schema.fields().iter().map(|f| f.name()).collect();
                        table.set_header(headers);

                        // mesodb-cli/src/main.rs inside the batch loop:

                        for batch in batches {
                            for row_idx in 0..batch.num_rows() {
                                let mut row = Vec::new();
                                for col_idx in 0..batch.num_columns() {
                                    let col = batch.column(col_idx);
                                    // This is a helper to turn Arrow values into strings
                                    let val =
                                        datafusion::arrow::util::display::array_value_to_string(
                                            col, row_idx,
                                        )?;
                                    row.push(val);
                                }
                                table.add_row(row);
                            }
                        }
                        println!("{table}");
                    }
                    Err(e) => println!("Error: {e}"),
                }
                let _ = rl.add_history_entry(input);
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}
