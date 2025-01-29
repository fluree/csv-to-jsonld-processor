use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::ProcessorError;
use crate::types::{FlureeDataModel, JsonLdVocabulary, VocabularyMap};
use crate::Manifest;

pub struct VocabularySerializer {
    manifest: Arc<Manifest>,
}

impl VocabularySerializer {
    pub fn new(manifest: Arc<Manifest>) -> Self {
        Self { manifest }
    }

    pub async fn save_vocabulary(
        &self,
        vocabulary: VocabularyMap,
        output_path: &PathBuf,
    ) -> Result<(), ProcessorError> {
        let ledger = self.manifest.ledger.clone();
        let label = self.manifest.name.clone();
        let comment = self.manifest.description.clone();

        let insert = FlureeDataModel {
            type_: vec!["f:DataModel".to_string()],
            id: self.manifest.id.clone(),
            label,
            comment,
            classes: vocabulary.classes.into_values().collect(),
            properties: vocabulary.properties.into_values().collect(),
        };

        let vocabulary = JsonLdVocabulary {
            context: serde_json::json!({
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "f": "https://ns.flur.ee/ledger#",
                "rdfs:domain": { "@type": "@id" },
                "rdfs:range": { "@type": "@id" },
                "rdfs:subClassOf": { "@type": "@id" },
                "f:oneOf": { "@type": "@id" },
            }),
            ledger,
            insert,
        };

        let vocab_json = serde_json::to_string_pretty(&vocabulary).map_err(|e| {
            ProcessorError::Processing(format!("Failed to serialize vocabulary: {}", e))
        })?;

        let output_dir = if output_path.is_dir() {
            output_path.clone()
        } else {
            // If the path doesn't exist yet, assume it's intended to be a directory
            Path::new(&output_path).to_path_buf()
        };

        // Create the full file path: output_dir/vocabulary.jsonld
        let output_path = output_dir.join("vocabulary.jsonld");

        // Ensure the directory exists
        fs::create_dir_all(&output_dir).map_err(|e| {
            ProcessorError::Processing(format!(
                "Failed to create directory for vocabulary file: {}",
                e
            ))
        })?;

        // Write the JSON to the file
        fs::write(&output_path, vocab_json).map_err(|e| {
            ProcessorError::Processing(format!("Failed to write vocabulary file: {}", e))
        })?;

        let output_path_str = output_path.to_string_lossy();
        tracing::info!("Saved vocabulary to {}", output_path_str);

        Ok(())
    }

    pub async fn save_vocabulary_meta(
        &self,
        vocabulary: &VocabularyMap,
        output_path: &PathBuf,
    ) -> Result<(), ProcessorError> {
        let encoded = bincode::serialize(vocabulary).map_err(|e| {
            ProcessorError::Processing(format!("Failed to serialize vocabulary: {}", e))
        })?;

        let output_dir = if output_path.is_dir() {
            output_path.clone()
        } else {
            // If the path doesn't exist yet, assume it's intended to be a directory
            Path::new(&output_path).to_path_buf()
        };

        // Create the full file path: output_dir/vocabulary.jsonld
        let output_path = output_dir.join("data_model.bincode");

        // Ensure the directory exists
        fs::create_dir_all(&output_dir).map_err(|e| {
            ProcessorError::Processing(format!(
                "Failed to create directory for vocabulary metadata file: {}",
                e
            ))
        })?;

        // Write the JSON to the file
        fs::write(&output_path, encoded).map_err(|e| {
            ProcessorError::Processing(format!("Failed to write vocabulary metadata file: {}", e))
        })?;

        let output_path_str = output_path.to_string_lossy();
        tracing::info!("Saved vocabulary metadata to {}", output_path_str);

        Ok(())
    }
}
