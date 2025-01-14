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
    pub fn new(manifest: Arc<Manifest>, is_strict: bool) -> Self {
        Self {
            processor: processor::VocabularyProcessor::new(Arc::clone(&manifest), is_strict),
            serializer: serializer::VocabularySerializer::new(manifest),
        }
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
}
