use spinoff::{Color, Spinner, Streams, spinners};
use std::io::IsTerminal;

pub struct Progress {
    spinner: Option<Spinner>,
}

#[derive(Clone, Copy)]
pub enum Style {
    Arc,
    Dots,
    Line,
}

impl Progress {
    pub fn start(style: Style, message: &'static str) -> Self {
        let interactive = std::io::stdout().is_terminal()
            && std::io::stderr().is_terminal()
            && std::env::var_os("TERM").is_none_or(|term| term != "dumb");
        let spinner = interactive.then(|| match style {
            Style::Arc => {
                Spinner::new_with_stream(spinners::Arc, message, Color::Cyan, Streams::Stderr)
            }
            Style::Dots => {
                Spinner::new_with_stream(spinners::Dots, message, Color::Cyan, Streams::Stderr)
            }
            Style::Line => {
                Spinner::new_with_stream(spinners::Line, message, Color::Cyan, Streams::Stderr)
            }
        });
        Self { spinner }
    }

    pub fn success(mut self, message: &str) {
        if let Some(spinner) = &mut self.spinner {
            spinner.success(message);
        }
        self.spinner = None;
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        if let Some(spinner) = &mut self.spinner {
            spinner.clear();
        }
    }
}
