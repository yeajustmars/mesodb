// src/tutor/mod.rs

use comfy_table::{Cell, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};
use mesodb_core::config::Config;
use mesodb_core::db::{MesoDb, OutputFormat, QueryOptions};
use mesodb_core::schema::SchemaMap;
// use mesodb_core::transactor::Fact;
use tempfile::TempDir;

pub mod hr_story;

pub struct TutorStory {
    pub id: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    pub steps: Vec<Step>,
}

pub struct Step {
    pub step_number: usize,
    pub title: &'static str,
    pub narrative: &'static str,
    pub challenge: &'static str,
    pub hint: &'static str,
    pub auto_fixture: Option<&'static str>,
    pub expected_query_substrings: Vec<&'static str>,
}

pub struct TutorSession {
    pub story: TutorStory,
    pub current_step: usize,
    pub db: MesoDb,
    pub _temp_dir: TempDir, // Kept alive so the DB directory isn't deleted during the session
}

impl TutorSession {
    pub async fn new(story: TutorStory) -> Self {
        let temp_dir = tempfile::tempdir().expect("Failed to create sandbox directory");
        let db_path = temp_dir.path().join("sandbox.db");

        // We boot the engine with a loose JIT schema for the tutorial sandbox
        let config = Config::default();
        let schema = SchemaMap::new();
        let db = MesoDb::open(db_path, schema, config).expect("Failed to boot embedded MesoDB");

        let session = Self {
            story,
            current_step: 0,
            db,
            _temp_dir: temp_dir,
        };

        // Load the initial fixture for step 1 if it exists
        session.load_current_fixture().await;
        session
    }

    async fn load_current_fixture(&self) {
        if let Some(step) = self.story.steps.get(self.current_step)
            && let Some(edn) = step.auto_fixture
        {
            println!("⏳ [System] Provisioning sandbox data...");
            match crate::edn::parse_edn_tx(edn) {
                Ok(facts) => {
                    let _ = self.db.transact(facts).await;
                }
                Err(e) => println!("⚠️ Sandbox Setup Error: {}", e),
            }
        }
    }

    pub fn start(&self) {
        println!("\n📚 Starting Tutorial: {}", self.story.title);
        println!("{}\n", self.story.description);
        self.print_current_step();
    }

    pub fn print_current_step(&self) {
        if let Some(step) = self.story.steps.get(self.current_step) {
            println!("==========================================");
            println!("STEP {}: {}", step.step_number, step.title);
            println!("==========================================");
            println!("{}\n", step.narrative);
            println!("🎯 CHALLENGE: {}", step.challenge);
            println!("(Type .hint if you get stuck, or .skip to move on)");
        } else {
            println!(
                "🎉 You have completed {}! Returning to offline mode.",
                self.story.title
            );
        }
    }

    /// Returns true if the session is still active, false if the story is over
    pub async fn process_input(&mut self, input: &str) -> bool {
        let step = match self.story.steps.get(self.current_step) {
            Some(s) => s,
            None => return false,
        };

        if input == ".hint" {
            println!("💡 HINT:\n{}\n", step.hint);
            return true;
        }

        if input == ".skip" {
            println!("⏭️  Skipping step...\n");
            self.current_step += 1;
            self.load_current_fixture().await;
            self.print_current_step();
            return self.current_step < self.story.steps.len();
        }

        // --- THE REAL ENGINE EXECUTION ---
        let is_query = input.replace(" ", "").starts_with("[:find");
        let mut executed_cleanly = false;

        if is_query {
            let opts = QueryOptions {
                format: OutputFormat::Tabular,
                history: input.contains("history"), // Basic history detection
                as_of: None,
                rules: None,
            };

            // FIX: Use query_native so we can easily iterate the rows!
            match self.db.query_native_with_options(input, opts).await {
                Ok(results) => {
                    if results.is_empty() {
                        println!("(0 rows returned)\n");
                    } else {
                        let mut table = Table::new();
                        table
                            .load_preset(UTF8_FULL)
                            .apply_modifier(UTF8_ROUND_CORNERS);

                        // Extract headers from the keys of the first row
                        let mut headers: Vec<String> = results[0].keys().cloned().collect();
                        headers.sort(); // Sort to keep column order deterministic
                        table.set_header(&headers);

                        // FIX: Actually populate the rows!
                        for row in &results {
                            let mut table_row = Vec::new();
                            for h in &headers {
                                let val_str = match row.get(h) {
                                    Some(v) => v.to_string(), // Uses our Value Display trait
                                    None => "null".to_string(),
                                };
                                table_row.push(Cell::new(val_str));
                            }
                            table.add_row(table_row);
                        }
                        println!("{table}");
                    }
                    executed_cleanly = true;
                }
                Err(e) => {
                    println!("❌ Database Error: {:?}", e);
                }
            }
        } else {
            match crate::edn::parse_edn_tx(input) {
                Ok(facts) => match self.db.transact(facts).await {
                    Ok(report) => {
                        println!(
                            "💾 Transaction committed! (Tx: {}, Datoms: {})\n",
                            report.tx_id, report.datoms_written
                        );
                        executed_cleanly = true;
                    }
                    Err(e) => {
                        println!("❌ Database Error: {:?}", e);
                    }
                },
                Err(e) => {
                    println!("❌ EDN Syntax Error: {}", e);
                }
            }
        }

        // --- VALIDATION ---
        let mut passed_structural = true;
        for substring in &step.expected_query_substrings {
            if !input.contains(substring) {
                passed_structural = false;
                break;
            }
        }

        if executed_cleanly && passed_structural {
            println!("✅ SUCCESS!\n");
            self.current_step += 1;
            self.load_current_fixture().await;
            self.print_current_step();
            self.current_step < self.story.steps.len()
        } else if !executed_cleanly {
            println!("Fix the database error above and try again.\n");
            true
        } else {
            // FIX: The Exploration UX
            println!("🔍 [Exploration Mode] Command executed successfully.");
            println!(
                "👉 The results didn't solve the current challenge, but keep digging! (Type .hint if stuck)\n"
            );
            true
        }
    }
}
