// src/repl.rs

use reedline::{Prompt, PromptEditMode, PromptHistorySearch, ValidationResult, Validator};
use std::borrow::Cow;

#[derive(Clone)]
pub struct MesoPrompt {
    pub state_label: String,
}

impl MesoPrompt {
    pub fn new(state_label: String) -> Self {
        Self { state_label }
    }
}

impl Prompt for MesoPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.state_label)
    }

    fn render_prompt_right(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn render_prompt_indicator(&self, _edit_mode: PromptEditMode) -> Cow<'_, str> {
        Cow::Borrowed("> ")
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
        // The magic! Virtual indentation without injecting physical spaces into the buffer.
        Cow::Borrowed("             > ")
    }

    fn render_prompt_history_search_indicator(
        &self,
        _history_search: PromptHistorySearch,
    ) -> Cow<'_, str> {
        Cow::Borrowed("(search)> ")
    }
}

pub struct MesoValidator;

impl Validator for MesoValidator {
    fn validate(&self, line: &str) -> ValidationResult {
        let mut open_brackets = 0;
        let mut in_string = false;
        let mut escape = false;

        for c in line.chars() {
            if escape {
                escape = false;
                continue;
            }
            match c {
                '\\' => escape = true,
                '"' => in_string = !in_string,
                '[' | '{' | '(' if !in_string => open_brackets += 1,
                ']' | '}' | ')' if !in_string => open_brackets -= 1,
                _ => {}
            }
        }

        if open_brackets > 0 || in_string {
            ValidationResult::Incomplete
        } else {
            ValidationResult::Complete
        }
    }
}
