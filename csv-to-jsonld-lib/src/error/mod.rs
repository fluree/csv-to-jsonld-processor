use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info, warn};

use crate::manifest::StorageLocation;

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
    #[error("Path processing error: {0}")]
    PathConversion(String),
}

impl From<ProcessorError> for ProcessingMessage {
    fn from(error: ProcessorError) -> Self {
        match error {
            ProcessorError::Io(e) => {
                ProcessingMessage::new(e.to_string(), Some("io_operation".into()))
            }
            ProcessorError::Json(e) => {
                ProcessingMessage::new(e.to_string(), Some("json_serialization".into()))
            }
            ProcessorError::Csv(e) => {
                ProcessingMessage::new(e.to_string(), Some("csv_parsing".into()))
            }
            ProcessorError::InvalidManifest(e) => {
                ProcessingMessage::new(e, Some("manifest_validation".into()))
            }
            ProcessorError::Processing(e) => {
                ProcessingMessage::new(e, Some("processing_data".into()))
            }
            ProcessorError::PathConversion(e) => {
                ProcessingMessage::new(e, Some("path_conversion".into()))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingMessage {
    source: Option<String>,
    message: String,
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

impl From<anyhow::Error> for ProcessingMessage {
    fn from(error: anyhow::Error) -> Self {
        ProcessingMessage::new(error.to_string(), error.source().map(|s| s.to_string()))
    }
}

impl Display for ProcessingState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.has_warnings() {
            writeln!(f, "Warnings:")?;
            for warning in &self.warnings {
                writeln!(f, "  {}", warning.message)?;
            }
        }

        if self.has_errors() {
            writeln!(f, "Errors:")?;
            for error in &self.errors {
                writeln!(f, "  {}", error.message)?;
            }
        }

        Ok(())
    }
}

impl std::error::Error for ProcessingState {}

impl ProcessingState {
    pub fn new() -> Self {
        Self {
            warnings: Vec::new(),
            errors: Vec::new(),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn add_warning(&mut self, message: impl Into<String>, source: Option<String>) {
        self.warnings.push(ProcessingMessage::new(message, source));
    }

    pub fn add_error(&mut self, message: impl Into<String>, source: Option<String>) {
        self.errors.push(ProcessingMessage::new(message, source));
    }

    pub fn add_error_from<T: Into<ProcessingMessage>>(&mut self, error: T) {
        self.errors.push(error.into());
    }

    pub fn add_warning_from<T: Into<ProcessingMessage>>(&mut self, error: T) {
        self.warnings.push(error.into());
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
    pub fn new() -> Self {
        ProcessingOutcome::Success
    }

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

    pub fn take_warnings(self) -> Vec<ProcessingMessage> {
        match self {
            ProcessingOutcome::Success => Vec::new(),
            ProcessingOutcome::SuccessWithWarnings(mut warnings) => warnings.drain(..).collect(),
            ProcessingOutcome::Failure { mut warnings, .. } => warnings.drain(..).collect(),
        }
    }

    pub fn take_errors(self) -> Vec<ProcessingMessage> {
        match self {
            ProcessingOutcome::Success => Vec::new(),
            ProcessingOutcome::SuccessWithWarnings(_) => Vec::new(),
            ProcessingOutcome::Failure { mut errors, .. } => errors.drain(..).collect(),
        }
    }

    /// Returns a tuple of errors and warnings
    pub fn take_messages(self) -> (Vec<ProcessingMessage>, Vec<ProcessingMessage>) {
        match self {
            ProcessingOutcome::Success => (Vec::new(), Vec::new()),
            ProcessingOutcome::SuccessWithWarnings(warnings) => (Vec::new(), warnings),
            ProcessingOutcome::Failure { errors, warnings } => (errors, warnings),
        }
    }

    pub fn merge_outcome(self, other: ProcessingOutcome) -> Self {
        let (other_errors, other_warnings) = other.take_messages();
        let (errors, warnings) = self.take_messages();
        let state = ProcessingState {
            warnings: warnings.into_iter().chain(other_warnings).collect(),
            errors: errors.into_iter().chain(other_errors).collect(),
        };
        ProcessingOutcome::from_state(state)
    }

    pub fn report(self) -> Result<(), anyhow::Error> {
        match self {
            ProcessingOutcome::Success => {
                info!("Processing completed successfully");
            }
            ProcessingOutcome::SuccessWithWarnings(warnings) => {
                warn!("Processing completed with warnings:");
                for warning in warnings {
                    if let Some(source) = warning.source {
                        warn!("[{}] {}", source, warning.message);
                    } else {
                        warn!("{}", warning.message);
                    }
                }
            }
            ProcessingOutcome::Failure { errors, warnings } => {
                if !warnings.is_empty() {
                    warn!("--- Warnings ---");
                    for warning in warnings {
                        if let Some(source) = warning.source {
                            warn!("[{}] {}", source, warning.message);
                        } else {
                            warn!("{}", warning.message);
                        }
                    }
                }

                error!("--- Errors ---");
                for error in errors {
                    if let Some(source) = error.source {
                        error!("[{}] {}", source, error.message);
                    } else {
                        error!("{}", error.message);
                    }
                }
                anyhow::bail!("Processing failed with errors");
            }
        };
        Ok(())
    }
}
