// mesodb-doc/src/lib.rs

use pulldown_cmark::{Options, Parser};
use pulldown_cmark_mdcat::resources::NoopResourceHandler;
use pulldown_cmark_mdcat::{Environment, Settings, TerminalCapabilities, TerminalSize, Theme};
use rust_embed::RustEmbed;
use std::env;
use std::io::{self, Write};
use syntect::{highlighting::ThemeSet, parsing::SyntaxSet};
use thiserror::Error;

#[derive(RustEmbed, Clone, Debug)]
#[folder = "./doc/"]
pub struct Doc;

#[derive(Error, Debug)]
pub enum MesoDocError {
    #[error("Failed setting up environment: {0}")]
    EnvError(String),
    #[error("Unknown doc: {0}")]
    UnknownDoc(String),
    #[error("Failed to read doc: {0}")]
    FailedRead(String),
    #[error("Failed to render doc: {0}")]
    RenderError(String),
}

pub fn get_doc(name: &str) -> Result<String, MesoDocError> {
    let mut name = String::from(name);
    if !name.ends_with(".md") {
        name.push_str(".md");
    }

    match Doc::get(&name) {
        Some(file) => {
            let bytes = file.data.to_vec();
            let s =
                String::from_utf8(bytes).map_err(|e| MesoDocError::FailedRead(e.to_string()))?;
            Ok(s)
        }
        None => Err(MesoDocError::UnknownDoc(format!("File not found: {name}"))),
    }
}

/// Reads the specified embedded documentation file and prints it to the terminal with full coloring
pub fn print_doc_to_terminal(doc_name: &str) -> Result<(), MesoDocError> {
    let content = get_doc(doc_name)?;

    let mut options = Options::empty();
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TASKLISTS);

    let parser = Parser::new_ext(&content, options);

    let terminal_size = TerminalSize::detect().unwrap_or_default();
    let terminal_capabilities = TerminalCapabilities::default();

    let current_dir = env::current_dir().map_err(|e| MesoDocError::EnvError(e.to_string()))?;

    let env = Environment::for_local_directory(&current_dir)
        .map_err(|e| MesoDocError::EnvError(e.to_string()))?;

    // 1. Load the defaults for both the syntaxes AND the visual theme collections
    let syntax_set = SyntaxSet::load_defaults_newlines();
    let theme_set = ThemeSet::load_defaults();

    // 2. Extract one of syntect's beautiful default themes.
    // "Base16-Ocean.dark" matches standard modern terminals beautifully.
    // You could also try "Solarized (dark)" or "InspiredGitHub".
    let syntax_theme = theme_set.themes.get("Base16-Ocean.dark").cloned();

    // Theme::default() supplies standard general styling (bold headers, link rules, etc.)
    let theme = Theme::default();

    // 3. Replace `None` with the resolved option reference
    let settings = Settings {
        terminal_capabilities,
        terminal_size,
        theme,
        syntax_theme,
        syntax_set: &syntax_set,
    };

    let stdout = io::stdout();
    let mut handle = stdout.lock();
    let resource_handler = NoopResourceHandler;

    pulldown_cmark_mdcat::push_tty(&settings, &env, &resource_handler, &mut handle, parser)
        .map_err(|e| MesoDocError::RenderError(e.to_string()))?;

    handle
        .flush()
        .map_err(|e| MesoDocError::RenderError(e.to_string()))?;

    Ok(())
}
