// src/tutor/hr_story.rs

use super::{Step, TutorStory};

pub fn build() -> TutorStory {
    TutorStory {
        id: "apex-hr",
        title: "The ApexCorp Payroll Saga",
        description: "Learn bitemporality by fixing a retroactive HR promotion without destroying the accounting audit trail.",
        steps: vec![
            Step {
                step_number: 1,
                title: "The Graph (Basic Assertions)",
                narrative: "Welcome to ApexCorp HR. You've just inherited a database tracking our org chart.\n\nEverything in MesoDB is an Entity-Attribute-Value (EAV) fact. To find out what we currently pay Alice, we query the graph for her `:employee/salary` attribute.",
                challenge: "Write a Datalog query to find Alice's current salary.\nFind the variables ?e and ?salary where the name is \"Alice\".",
                hint: "[:find ?salary :where [?e :employee/name \"Alice\"] [?e :employee/salary ?salary]]",
                auto_fixture: Some(
                    r#"
                    [
                        [:db/add 100 :employee/name "Alice"]
                        [:db/add 100 :employee/salary 90000]
                        [:db/add 200 :employee/name "Bob"]
                        [:db/add 200 :employee/manager 100]
                    ]
                "#,
                ),
                expected_query_substrings: vec![":employee/name", "Alice", ":employee/salary"],
            },
            Step {
                step_number: 2,
                title: "Implicit Joins (Walking the Graph)",
                narrative: "MesoDB doesn't use SQL-style JOINs. References point directly to Entity IDs, allowing you to walk the graph effortlessly.\n\nAlice is a manager. Let's see who reports to her.",
                challenge: "Write a query to find the names of employees whose `:employee/manager` is Alice's Entity ID.",
                hint: "[:find ?report_name :where [?manager :employee/name \"Alice\"] [?report :employee/manager ?manager] [?report :employee/name ?report_name]]",
                auto_fixture: None,
                expected_query_substrings: vec![":employee/manager", ":employee/name"],
            },
            Step {
                step_number: 3,
                title: "Rewriting History (Valid Time)",
                narrative: "CRITICAL HR ERROR: Alice was promoted to VP of Engineering on March 1st (Valid Time), bumping her salary to 125,000.\n\nHowever, the HR admin forgot to file the paperwork, and it's now March 15th! If we do a standard UPDATE, we lose the fact that for two weeks, we *thought* her salary was 90k.",
                challenge: "Execute a transaction to retroactively update Alice's salary to 125000, using a Valid Time of March 1st.",
                hint: "[[:db/add 100 :employee/salary 125000 #inst \"2026-03-01T00:00:00Z\"]]",
                auto_fixture: None,
                expected_query_substrings: vec![":db/add", "100", ":employee/salary", "125000"],
            },
            Step {
                step_number: 4,
                title: "The Quantum Audit (Transaction Time)",
                narrative: "Finance is running an audit. They want to know exactly what the system *believed* Alice's salary was on March 14th, before HR fixed the mistake.\n\nBy passing an `as_of` Transaction Time modifier to our query, MesoDB instantly forks the timeline to show us the historical state.",
                challenge: "Run a query to find Alice's salary, but append an `as_of` modifier for March 14th.",
                hint: "[:find ?salary :where [?e :employee/name \"Alice\"] [?e :employee/salary ?salary]] as_of \"2026-03-14T00:00:00Z\"",
                auto_fixture: None,
                expected_query_substrings: vec!["as_of", "2026-03-14"],
            },
            Step {
                step_number: 5,
                title: "The Purge (Option A Compaction)",
                narrative: "Our bitemporal history is mathematically perfect, but disks aren't infinite. Old retractions are cluttering our Parquet files.\n\nOption A Compaction uses a background `LEAD()` query to physically crush dead space without altering the mathematical truth of the database.",
                challenge: "Type the meta-command to trigger background compaction.",
                hint: ".compact",
                auto_fixture: None,
                expected_query_substrings: vec![".compact"],
            },
        ],
    }
}
