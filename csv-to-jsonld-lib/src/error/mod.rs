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
