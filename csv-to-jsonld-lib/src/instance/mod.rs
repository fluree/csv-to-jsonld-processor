mod processor_impl;
mod serializer;
mod types;
mod validation;
mod value_processor;

pub(crate) use types::InstanceProcessor;

use std::path::PathBuf;
use std::sync::Arc;

use crate::error::{ProcessingState, ProcessorError};
use crate::manifest::{ImportStep, StorageLocation};
use crate::types::VocabularyMap;
use crate::Manifest;

pub use crate::types::{JsonLdInstance, JsonLdInstances};

pub struct InstanceManager {
    pub processor: InstanceProcessor,
    pub serializer: serializer::InstanceSerializer,
}

impl InstanceManager {
    pub fn new(manifest: Arc<Manifest>, is_strict: bool, model_base_iri: String) -> Self {
        Self {
            processor: InstanceProcessor::new(
                Arc::clone(&manifest),
                is_strict,
                model_base_iri.clone(),
            ),
            serializer: serializer::InstanceSerializer::new(manifest, model_base_iri),
        }
    }

    pub fn set_vocabulary(&mut self, vocabulary: VocabularyMap) {
        self.processor.set_vocabulary(vocabulary);
    }

    pub fn take_vocabulary(&mut self) -> (VocabularyMap, ProcessingState) {
        self.processor.take_vocabulary()
    }

    pub async fn process_simple_instance(
        &mut self,
        step: &ImportStep,
        // instance_path: &str,
        s3_client: Option<&aws_sdk_s3::Client>,
    ) -> Result<ProcessingState, ProcessorError> {
        self.processor
            // .process_simple_instance(step, instance_path)
            .process_simple_instance(step, s3_client)
            .await
    }

    pub async fn process_subclass_instance(
        &mut self,
        step: &ImportStep,
        // instance_path: &str,
        s3_client: Option<&aws_sdk_s3::Client>,
    ) -> Result<ProcessingState, ProcessorError> {
        self.processor
            // .process_subclass_instance(step, instance_path)
            .process_subclass_instance(step, s3_client)
            .await
    }

    pub async fn process_properties_instance(
        &mut self,
        step: &ImportStep,
        // instance_path: &str,
        s3_client: Option<&aws_sdk_s3::Client>,
    ) -> Result<ProcessingState, ProcessorError> {
        self.processor
            // .process_properties_instance(step, instance_path)
            .process_properties_instance(step, s3_client)
            .await
    }

    pub async fn save_instances(
        &self,
        output_path: &StorageLocation,
        s3_client: Option<&aws_sdk_s3::Client>,
    ) -> Result<(), ProcessorError> {
        let vocabulary = self.processor.get_vocabulary();
        self.serializer
            .save_instances(
                self.processor.get_instances(),
                output_path,
                vocabulary,
                s3_client,
            )
            .await
    }
}
