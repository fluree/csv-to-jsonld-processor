mod processor;
mod serializer;

use std::path::PathBuf;
use std::sync::Arc;

use crate::error::ProcessorError;
use crate::manifest::ImportStep;
use crate::types::VocabularyMap;
use crate::Manifest;

pub use crate::types::{JsonLdInstance, JsonLdInstances};

pub struct InstanceManager {
    pub processor: processor::InstanceProcessor,
    pub serializer: serializer::InstanceSerializer,
}

impl InstanceManager {
    pub fn new(manifest: Arc<Manifest>, is_strict: bool) -> Self {
        Self {
            processor: processor::InstanceProcessor::new(Arc::clone(&manifest), is_strict),
            serializer: serializer::InstanceSerializer::new(manifest),
        }
    }

    pub fn set_vocabulary(&mut self, vocabulary: VocabularyMap) {
        self.processor.set_vocabulary(vocabulary);
    }

    pub async fn process_simple_instance(
        &mut self,
        step: &ImportStep,
        instance_path: &str,
    ) -> Result<(), ProcessorError> {
        self.processor
            .process_simple_instance(step, instance_path)
            .await
    }

    pub async fn process_subclass_instance(
        &mut self,
        step: &ImportStep,
        instance_path: &str,
    ) -> Result<(), ProcessorError> {
        self.processor
            .process_subclass_instance(step, instance_path)
            .await
    }

    pub async fn process_properties_instance(
        &mut self,
        step: &ImportStep,
        instance_path: &str,
    ) -> Result<(), ProcessorError> {
        self.processor
            .process_properties_instance(step, instance_path)
            .await
    }

    pub async fn save_instances(&mut self, output_path: &PathBuf) -> Result<(), ProcessorError> {
        let vocabulary = self.processor.get_vocabulary();
        self.serializer
            .save_instances(self.processor.get_instances(), output_path, vocabulary)
            .await
    }
}
