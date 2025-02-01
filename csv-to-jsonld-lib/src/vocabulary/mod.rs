mod mapping;
mod processor;
mod serializer;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use processor::VocabularyProcessorMetadata;

use crate::error::ProcessorError;
use crate::manifest::{ImportStep, StorageLocation};
use crate::types::VocabularyMap;
use crate::{Manifest, ProcessingState};

pub struct VocabularyManager {
    pub processor: processor::VocabularyProcessor,
    serializer: serializer::VocabularySerializer,
}

impl VocabularyManager {
    pub async fn new(
        manifest: Arc<Manifest>,
        is_strict: bool,
        vocab_meta_path: Option<StorageLocation>,
        s3_client: Option<&aws_sdk_s3::Client>,
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
                    s3_client,
                )
                .await?
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
        // model_path: &str,
        s3_client: Option<&aws_sdk_s3::Client>,
    ) -> Result<ProcessingState, ProcessorError> {
        // self.processor.process_vocabulary(step, model_path).await
        self.processor.process_vocabulary(step, s3_client).await
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
        output_path: &StorageLocation,
        s3_client: Option<&aws_sdk_s3::Client>,
    ) -> Result<(), ProcessorError> {
        self.serializer
            .save_vocabulary(vocabulary, output_path, s3_client)
            .await
    }

    pub async fn save_vocabulary_meta(
        &mut self,
        vocabulary: &VocabularyMap,
        output_path: &StorageLocation,
        s3_client: Option<&aws_sdk_s3::Client>,
    ) -> Result<(), ProcessorError> {
        self.processor.vocabulary = vocabulary.clone();
        let strict_vocab_metadata = VocabularyProcessorMetadata::from(&self.processor);
        let encoded = serde_json::to_vec(&strict_vocab_metadata).map_err(|e| {
            ProcessorError::Processing(format!("Failed to serialize vocabulary metadata: {:#?}", e))
        })?;

        output_path.write_contents(&encoded, s3_client).await
    }
}
