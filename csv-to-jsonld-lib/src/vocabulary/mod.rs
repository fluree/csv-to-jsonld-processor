mod mapping;
mod processor;
mod serializer;

use std::path::PathBuf;
use std::sync::Arc;

use crate::error::ProcessorError;
use crate::manifest::ImportStep;
use crate::types::VocabularyMap;
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
        &self,
        vocabulary: &VocabularyMap,
        output_path: &PathBuf,
    ) -> Result<(), ProcessorError> {
        self.serializer
            .save_vocabulary_meta(vocabulary, output_path)
            .await
    }
}
