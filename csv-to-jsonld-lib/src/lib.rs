//! CSV to JSON-LD Processor Library
//!
//! This library provides functionality to convert CSV files to JSON-LD format
//! based on a manifest specification.

mod error;
mod instance;
mod manifest;
mod processor;
mod types;
mod vocabulary;

pub use error::ProcessorError;
pub use instance::{JsonLdInstance, JsonLdInstances};
pub use manifest::{ImportSection, ImportStep, Manifest};
pub use processor::Processor;
pub use types::{ColumnOverride, JsonLdContext, JsonLdVocabulary, VocabularyTerm};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;
    use tracing::{error, info};

    static INIT: Once = Once::new();

    /// Initialize logging exactly once for all tests
    fn init_logging() {
        INIT.call_once(|| {
            tracing_subscriber::fmt()
                .with_test_writer()
                .with_max_level(tracing::Level::DEBUG)
                .init();
        });
    }

    #[test]
    fn test_manifest_loading() {
        init_logging();

        info!("Testing manifest loading");
        let manifest = Manifest::from_file("../test-data/manifest.jsonld").unwrap();
        assert_eq!(manifest.type_, "CSVImportManifest");

        info!("Validating manifest");
        match manifest.validate() {
            Ok(_) => info!("Manifest validation successful"),
            Err(e) => error!("Manifest validation failed: {}", e),
        }
        assert!(manifest.validate().is_ok());
    }
}
