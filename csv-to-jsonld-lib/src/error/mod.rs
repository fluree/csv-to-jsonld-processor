use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProcessorError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),
    #[error("Invalid manifest: {0}")]
    InvalidManifest(String),
    #[error("Processing error: {0}")]
    Processing(String),
}

#[derive(Debug, Clone)]
pub struct ProcessingMessage {
    pub message: String,
    pub source: Option<String>,
}

impl ProcessingMessage {
    pub fn new(message: impl Into<String>, source: Option<String>) -> Self {
        Self {
            message: message.into(),
            source,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ProcessingState {
    warnings: Vec<ProcessingMessage>,
    errors: Vec<ProcessingMessage>,
}

impl ProcessingState {
    pub fn new() -> Self {
        Self {
            warnings: Vec::new(),
            errors: Vec::new(),
        }
    }

    pub fn add_warning(&mut self, message: impl Into<String>, source: Option<String>) {
        self.warnings.push(ProcessingMessage::new(message, source));
    }

    pub fn add_error(&mut self, message: impl Into<String>, source: Option<String>) {
        self.errors.push(ProcessingMessage::new(message, source));
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    pub fn get_warnings(&self) -> &[ProcessingMessage] {
        &self.warnings
    }

    pub fn get_errors(&self) -> &[ProcessingMessage] {
        &self.errors
    }

    pub fn merge(&mut self, other: ProcessingState) {
        self.warnings.extend(other.warnings);
        self.errors.extend(other.errors);
    }
}

#[derive(Debug)]
pub enum ProcessingOutcome {
    Success,
    SuccessWithWarnings(Vec<ProcessingMessage>),
    Failure {
        errors: Vec<ProcessingMessage>,
        warnings: Vec<ProcessingMessage>,
    },
}

impl ProcessingOutcome {
    pub fn from_state(state: ProcessingState) -> Self {
        if state.errors.is_empty() && state.warnings.is_empty() {
            ProcessingOutcome::Success
        } else if state.has_errors() {
            ProcessingOutcome::Failure {
                errors: state.errors,
                warnings: state.warnings,
            }
        } else {
            ProcessingOutcome::SuccessWithWarnings(state.warnings)
        }
    }
}
