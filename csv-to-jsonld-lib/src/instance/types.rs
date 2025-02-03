use crate::error::ProcessingState;
use crate::types::{JsonLdInstance, VocabularyMap};
use crate::Manifest;
use std::collections::HashMap;
use std::sync::Arc;

pub struct InstanceProcessor {
    pub(crate) manifest: Arc<Manifest>,
    pub(crate) instances: HashMap<String, JsonLdInstance>,
    pub(crate) vocabulary: Option<VocabularyMap>,
    pub(crate) is_strict: bool,
    pub(crate) ignore: HashMap<String, Vec<String>>,
    pub(crate) is_namespace_iris: bool,
    pub(crate) model_base_iri: String,
    pub(crate) instances_base_iri: String,
    pub(crate) processing_state: ProcessingState,
}

impl InstanceProcessor {
    pub fn new(manifest: Arc<Manifest>, is_strict: bool, model_base_iri: String) -> Self {
        let instances_base_iri = manifest.instances.base_iri.clone();
        let instances_base_iri = if instances_base_iri.is_empty() {
            model_base_iri.clone()
        } else {
            instances_base_iri
        };
        let ignore = manifest
            .instances
            .sequence
            .iter()
            .fold(HashMap::new(), |mut acc, step| {
                if let Some(ignores) = &step.ignore {
                    acc.insert(step.instance_type.clone(), ignores.clone());
                }
                acc
            });
        let is_namespace_iris = manifest.instances.namespace_iris;
        Self {
            manifest,
            instances: HashMap::new(),
            vocabulary: None,
            is_strict,
            ignore,
            is_namespace_iris,
            model_base_iri,
            instances_base_iri,
            processing_state: ProcessingState::new(),
        }
    }

    pub fn set_vocabulary(&mut self, vocabulary: VocabularyMap) {
        self.vocabulary = Some(vocabulary);
    }

    pub fn get_vocabulary(&self) -> &VocabularyMap {
        self.vocabulary.as_ref().unwrap()
    }

    pub fn take_vocabulary(&mut self) -> (VocabularyMap, ProcessingState) {
        let vocabulary = self.vocabulary.take().unwrap();
        let state = std::mem::take(&mut self.processing_state);
        (vocabulary, state)
    }

    pub fn get_instances(&self) -> &HashMap<String, JsonLdInstance> {
        &self.instances
    }
}
