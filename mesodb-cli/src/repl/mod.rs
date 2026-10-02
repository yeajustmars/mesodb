// mesodb-cli/src/repl/mod.rs

use nu_ansi_term::{Color, Style};
use reedline::{
    Completer, Highlighter, Prompt, PromptEditMode, PromptHistorySearch, Span, StyledText,
    Suggestion, ValidationResult, Validator,
};
use std::borrow::Cow;

// -----------------------------------------------------------------------------
// MesoPrompt: Dynamic Terminal Prompt
// -----------------------------------------------------------------------------

#[derive(Clone)]
pub struct MesoPrompt {
    pub target: String,
}

impl MesoPrompt {
    pub fn new(target: String) -> Self {
        Self { target }
    }
}

impl Prompt for MesoPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        let text = format!("meso({})", self.target);
        Cow::Owned(Style::new().bold().fg(Color::Cyan).paint(text).to_string())
    }

    fn render_prompt_right(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn render_prompt_indicator(&self, _edit_mode: PromptEditMode) -> Cow<'_, str> {
        Cow::Owned(
            Style::new()
                .bold()
                .fg(Color::Green)
                .paint(" ❯ ")
                .to_string(),
        )
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
        Cow::Owned(
            Style::new()
                .bold()
                .fg(Color::DarkGray)
                .paint(" ⋯ ")
                .to_string(),
        )
    }

    fn render_prompt_history_search_indicator(
        &self,
        _history_search: PromptHistorySearch,
    ) -> Cow<'_, str> {
        Cow::Owned(
            Style::new()
                .bold()
                .fg(Color::Yellow)
                .paint("(search) ❯ ")
                .to_string(),
        )
    }
}

// -----------------------------------------------------------------------------
// MesoValidator: Multiline Bracket Matcher
// -----------------------------------------------------------------------------

pub struct MesoValidator;

impl Validator for MesoValidator {
    fn validate(&self, line: &str) -> ValidationResult {
        let mut open_brackets = 0;
        let mut open_braces = 0;
        let mut open_parens = 0;
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
                '[' if !in_string => open_brackets += 1,
                ']' if !in_string => open_brackets -= 1,
                '{' if !in_string => open_braces += 1,
                '}' if !in_string => open_braces -= 1,
                '(' if !in_string => open_parens += 1,
                ')' if !in_string => open_parens -= 1,
                _ => {}
            }
        }

        if open_brackets > 0 || open_braces > 0 || open_parens > 0 || in_string {
            ValidationResult::Incomplete
        } else {
            ValidationResult::Complete
        }
    }
}

// -----------------------------------------------------------------------------
// MesoHighlighter: Real-time Datalog/EDN Syntax Highlighting
// -----------------------------------------------------------------------------

pub struct MesoHighlighter;

impl Highlighter for MesoHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> StyledText {
        let mut styled_text = StyledText::new();
        let mut buffer = String::new();
        let mut in_string = false;

        let flush_buffer = |buf: &mut String, styled: &mut StyledText| {
            if buf.is_empty() {
                return;
            }
            let style = if buf.starts_with(':') {
                Style::new().fg(Color::Magenta) // Keywords
            } else if buf.starts_with('?') {
                Style::new().fg(Color::Cyan) // Variables
            } else if buf.parse::<f64>().is_ok() {
                Style::new().fg(Color::Yellow) // Numbers
            } else if buf == "pull" || buf == "true" || buf == "false" {
                Style::new().fg(Color::LightBlue) // Built-ins
            } else {
                Style::new().fg(Color::White) // Standard text
            };
            styled.push((style, buf.clone()));
            buf.clear();
        };

        for c in line.chars() {
            if c == '"' {
                flush_buffer(&mut buffer, &mut styled_text);
                in_string = !in_string;
                buffer.push(c);
                if !in_string {
                    styled_text.push((Style::new().fg(Color::Green), buffer.clone()));
                    buffer.clear();
                }
                continue;
            }

            if in_string {
                buffer.push(c);
                continue;
            }

            if c.is_whitespace()
                || c == '['
                || c == ']'
                || c == '{'
                || c == '}'
                || c == '('
                || c == ')'
            {
                flush_buffer(&mut buffer, &mut styled_text);
                styled_text.push((Style::new().fg(Color::DarkGray), c.to_string()));
            } else {
                buffer.push(c);
            }
        }

        flush_buffer(&mut buffer, &mut styled_text);
        styled_text
    }
}

// -----------------------------------------------------------------------------
// MesoCompleter: Dot-Command Auto-Completion
// -----------------------------------------------------------------------------

pub struct MesoCompleter {
    commands: Vec<String>,
}

impl MesoCompleter {
    pub fn new() -> Self {
        Self {
            commands: vec![
                ".query ".into(),
                ".tx ".into(),
                ".status".into(),
                ".compact".into(),
                ".help".into(),
                ".exit".into(),
            ],
        }
    }
}

impl Completer for MesoCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let mut suggestions = Vec::new();
        if line.starts_with('.') {
            for cmd in &self.commands {
                if cmd.starts_with(line) {
                    suggestions.push(Suggestion {
                        value: cmd.clone(),
                        description: None,
                        style: None, // <-- ADDED: Satisfy the Reedline struct requirement
                        extra: None,
                        span: Span::new(0, pos),
                        append_whitespace: false,
                    });
                }
            }
        }
        suggestions
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validator_bracket_matching() {
        let validator = MesoValidator;

        // Complete
        assert!(matches!(
            validator.validate("[:find ?e]"),
            ValidationResult::Complete
        ));
        assert!(matches!(
            validator.validate("[[:db/add 1 :a 2]]"),
            ValidationResult::Complete
        ));

        // Incomplete
        assert!(matches!(
            validator.validate("[:find ?e"),
            ValidationResult::Incomplete
        ));
        assert!(matches!(
            validator.validate("[:find (pull ?e [*]"),
            ValidationResult::Incomplete
        ));

        // String escaping logic
        assert!(matches!(
            validator.validate("[:find ?e :where [?e :name \"[Bob]\"]]"),
            ValidationResult::Complete
        ));
        assert!(matches!(
            validator.validate("[:find ?e :where [?e :name \"Bob]]"),
            ValidationResult::Incomplete
        )); // Missing closing quote
    }

    #[test]
    fn test_highlighter_token_boundaries() {
        let hl = MesoHighlighter;

        // As long as this does not panic, the custom character lexer handles
        // strings, variables, and bracket boundaries without index out-of-bounds errors.
        let _styled = hl.highlight("[:find ?var 42 \"string with spaces\"]", 0);
    }
}
