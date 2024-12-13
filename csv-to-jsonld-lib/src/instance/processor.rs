use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use crate::error::ProcessorError;
use crate::manifest::ImportStep;
use crate::types::{JsonLdInstance, VocabularyMap};
use crate::Manifest;
use anyhow::Context;
use serde_json::Value;

pub struct InstanceProcessor {
    manifest: Arc<Manifest>,
    instances: HashMap<String, JsonLdInstance>,
    vocabulary: Option<VocabularyMap>,
    is_strict: bool,
    ignore: HashMap<String, Vec<String>>,
}

impl InstanceProcessor {
    pub fn new(manifest: Arc<Manifest>, is_strict: bool) -> Self {
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
        Self {
            manifest,
            instances: HashMap::new(),
            vocabulary: None,
            is_strict,
            ignore,
        }
    }

    pub fn set_vocabulary(&mut self, vocabulary: VocabularyMap) {
        self.vocabulary = Some(vocabulary);
    }

    pub fn get_vocabulary(&mut self) -> VocabularyMap {
        self.vocabulary.take().unwrap()
    }

    pub fn get_instances(&self) -> &HashMap<String, JsonLdInstance> {
        &self.instances
    }

    fn validate_headers(
        &self,
        headers: &[String],
        class_type: &str,
        identifier_label: &str,
    ) -> Result<(), ProcessorError> {
        let valid_labels = self.get_valid_property_labels(class_type);
        let mut unknown_headers = Vec::new();
        let ignorable_headers = match self.ignore.get(class_type) {
            Some(headers) => headers.as_slice(),
            None => &[],
        };

        for header in headers {
            // Skip if it's the identifier column
            if header == identifier_label {
                continue;
            }

            if !valid_labels.contains(header) && !ignorable_headers.contains(header) {
                unknown_headers.push(header.clone());
            }
        }

        if !unknown_headers.is_empty() {
            let message = format!(
                "Unknown columns found in CSV for class '{}': {:?}. These columns do not correspond to any properties defined in the vocabulary.",
                class_type, unknown_headers
            );

            if self.is_strict {
                return Err(ProcessorError::Processing(message));
            } else {
                tracing::warn!("{}", message);
            }
        }

        Ok(())
    }

    fn get_valid_property_labels(&self, class_type: &str) -> HashSet<String> {
        let mut valid_labels = HashSet::new();
        let vocab = self.vocabulary.as_ref().unwrap();
        let class_iri = format!("{}{}", self.manifest.model.base_iri, class_type);

        // Get properties from class's rdfs:range
        if let Some(class) = vocab
            .classes
            .values()
            .find(|c| c.id.final_iri() == class_iri)
        {
            if let Some(props) = &class.range {
                for prop_iri in props {
                    // Get property label from vocabulary
                    if let Some(prop) = vocab
                        .properties
                        .values()
                        .find(|p| p.id.final_iri() == *prop_iri)
                    {
                        valid_labels.insert(prop.label.clone());
                    }
                }
            }
        }

        // Get properties that have this class in their rdfs:domain
        for prop in vocab.properties.values() {
            if let Some(domains) = &prop.domain {
                if domains.contains(&class_iri) {
                    valid_labels.insert(prop.label.clone());
                }
            }
        }

        valid_labels
    }

    fn drop_ignore(
        &self,
        headers: &mut [String],
        class_type: &str,
        identifier_label: &str,
    ) -> Vec<String> {
        let ignorable_headers = match self.ignore.get(class_type) {
            Some(headers) => headers.as_slice(),
            None => &[],
        };

        headers
            .iter()
            .filter(|h| !ignorable_headers.contains(h) && *h != identifier_label)
            .cloned()
            .collect()
    }

    pub async fn process_simple_instance(
        &mut self,
        step: &ImportStep,
        instance_path: &str,
    ) -> Result<(), ProcessorError> {
        // Get the class type from the manifest
        let class_type = step.instance_type.clone();

        // Get the vocabulary map
        let vocab = self.vocabulary.as_ref().ok_or_else(|| {
            ProcessorError::Processing("Vocabulary must be set before processing instances".into())
        })?;

        tracing::debug!("Getting identifier label for class '{}'", class_type);

        // Get the identifier property for this class
        let identifier_label = vocab.get_identifier_label(&class_type).ok_or_else(|| {
            tracing::debug!("[Error finding identifier label] {:#?}", vocab.identifiers.keys());
            ProcessorError::Processing(format!(
                "No identifier property found for class '{}'. To import instances of this class, your data model CSVs must indicate which column of the instance data will be used as the identifier (\"@id\") for instances of this class.",
                class_type
            ))
        })?;

        let file_path = PathBuf::from(instance_path).join(&step.path);
        tracing::debug!("Reading instance data from {:?}", file_path);

        let mut rdr = csv::Reader::from_path(&file_path).map_err(|e| {
            ProcessorError::Processing(format!(
                "Failed to read CSV @ {}: {}",
                &file_path.to_string_lossy(),
                e
            ))
        })?;

        // Read headers first and collect them into a Vec
        let mut headers: Vec<String> = rdr
            .headers()
            .map_err(|e| ProcessorError::Processing(format!("Failed to read CSV headers: {}", e)))?
            .iter()
            .map(|h| h.to_string())
            .collect();

        // Validate headers against vocabulary
        self.validate_headers(&headers, &class_type, identifier_label)?;

        // Find the identifier column index
        let id_column_index = headers
            .iter()
            .position(|h| h == identifier_label)
            .ok_or_else(|| {
                ProcessorError::Processing(format!(
                    "Identifier column '{}' not found in headers",
                    identifier_label
                ))
            })?;

        let headers = self.drop_ignore(&mut headers, &class_type, identifier_label);

        // Process each row
        for result in rdr.records() {
            let record = result.map_err(|e| {
                ProcessorError::Processing(format!("Failed to read CSV record: {}", e))
            })?;

            // Get the identifier value
            let id = record
                .get(id_column_index)
                .ok_or_else(|| ProcessorError::Processing("Missing identifier value".into()))?;

            let mut properties = serde_json::Map::new();

            // Map CSV columns to JSON-LD properties
            for (i, header) in headers.iter().enumerate() {
                if let Some(value) = record.get(i) {
                    // Skip empty values
                    if !value.is_empty() {
                        // Handle numeric values
                        if value.contains('$') || value.contains('%') {
                            let cleaned_value = value.replace(['$', '%', ','], "");
                            if let Ok(num) = cleaned_value.parse::<f64>() {
                                properties.insert(
                                    header.to_string(),
                                    Value::Number(serde_json::Number::from_f64(num).unwrap()),
                                );
                                continue;
                            }
                        }
                        // Default to string values
                        properties.insert(header.to_string(), Value::String(value.to_string()));
                    }
                }
            }

            let instance = JsonLdInstance {
                id: id.to_string(),
                type_: vec![class_type.clone()],
                properties,
            };

            self.update_or_insert_instance(instance)?;
        }

        Ok(())
    }

    fn update_or_insert_instance(
        &mut self,
        instance: JsonLdInstance,
    ) -> Result<(), ProcessorError> {
        let id = instance.id.clone();

        match self.instances.entry(id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().update_with(instance)?;
            }
            Entry::Vacant(entry) => {
                entry.insert(instance);
            }
        }

        Ok(())
    }
}
