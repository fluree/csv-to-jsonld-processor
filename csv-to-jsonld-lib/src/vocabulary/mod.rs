mod mapping;
mod processor;
mod serializer;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use processor::VocabularyProcessorMetadata;

use crate::error::ProcessorError;
use crate::manifest::ImportStep;
use crate::types::{StrictVocabularyMap, VocabularyMap};
use crate::Manifest;

pub struct VocabularyManager {
    pub processor: processor::VocabularyProcessor,
    serializer: serializer::VocabularySerializer,
}

impl VocabularyManager {
    pub fn new(
        manifest: Arc<Manifest>,
        is_strict: bool,
        vocab_meta_path: Option<PathBuf>,
    ) -> Result<Self, ProcessorError> {
        let is_manifest_vocab_empty = manifest.model.sequence.is_empty();
        let processor = if is_manifest_vocab_empty {
            if let Some(vocab_meta_path) = vocab_meta_path {
                tracing::info!(
                    "No vocabulary in manifest, loading from vocab meta file: {:?}",
                    vocab_meta_path
                );
                processor::VocabularyProcessor::new_from_vocab_meta(
                    Arc::clone(&manifest),
                    &vocab_meta_path,
                    is_strict,
                )?
            } else {
                processor::VocabularyProcessor::new(Arc::clone(&manifest), is_strict)
            }
        } else {
            processor::VocabularyProcessor::new(Arc::clone(&manifest), is_strict)
        };

        Ok(Self {
            processor,
            serializer: serializer::VocabularySerializer::new(manifest),
        })
    }

    pub async fn process_vocabulary(
        &mut self,
        step: ImportStep,
        model_path: &str,
    ) -> Result<(), ProcessorError> {
        self.processor.process_vocabulary(step, model_path).await
    }

    // pub async fn process_subclass_vocabulary(
    //     &mut self,
    //     step: &ImportStep,
    //     model_path: &str,
    // ) -> Result<(), ProcessorError> {
    //     self.processor
    //         .process_subclass_vocabulary(step, model_path)
    //         .await
    // }

    pub async fn save_vocabulary(
        &self,
        vocabulary: VocabularyMap,
        output_path: &PathBuf,
    ) -> Result<(), ProcessorError> {
        self.serializer
            .save_vocabulary(vocabulary, output_path)
            .await
    }

    pub async fn save_vocabulary_meta(
        &mut self,
        vocabulary: &VocabularyMap,
        output_path: &PathBuf,
    ) -> Result<(), ProcessorError> {
        self.processor.vocabulary = vocabulary.clone();
        let strict_vocab_metadata = VocabularyProcessorMetadata::from(&self.processor);
        let encoded = serde_json::to_value(&strict_vocab_metadata)
            .map_err(|e| {
                ProcessorError::Processing(format!(
                    "Failed to serialize vocabulary metadata: {:#?}",
                    e
                ))
            })?
            .to_string();

        let output_dir = if output_path.is_dir() {
            output_path.clone()
        } else {
            // If the path doesn't exist yet, assume it's intended to be a directory
            Path::new(&output_path).to_path_buf()
        };

        // Create the full file path: output_dir/vocabulary.jsonld
        let output_path = output_dir.join("data_model.tmp");

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
