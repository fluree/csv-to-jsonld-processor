use crate::error::ProcessorError;
use crate::manifest::ImportStep;
use crate::types::VocabularyMap;
use crate::Manifest;
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Serialize, Clone)]
pub struct JsonLdInstance {
    #[serde(rename = "@id")]
    id: String,
    #[serde(rename = "@type")]
    type_: Vec<String>,
    #[serde(flatten)]
    properties: serde_json::Map<String, serde_json::Value>,
}

impl JsonLdInstance {
    fn update_with(&mut self, other: JsonLdInstance) -> Result<(), ProcessorError> {
        // Types should be the same
        if self.type_ != other.type_ {
            return Err(ProcessorError::Processing(format!(
                "Conflicting types for instance '{}': {:?} vs {:?}",
                self.id, self.type_, other.type_
            )));
        }

        // Merge properties
        for (key, value) in other.properties {
            match self.properties.entry(key.clone()) {
                serde_json::map::Entry::Vacant(entry) => {
                    entry.insert(value);
                }
                serde_json::map::Entry::Occupied(mut entry) => {
                    let current: &mut serde_json::Value = entry.get_mut();
                    if current.is_array() {
                        // Add to existing array
                        let array = current.as_array_mut().unwrap();
                        let mut set: HashSet<Value> = array.drain(..).collect();
                        set.insert(value);
                        *array = set.into_iter().collect();
                    } else {
                        // Convert to array with both values
                        let mut set: HashSet<Value> = HashSet::new();
                        set.insert(current.clone());
                        set.insert(value);
                        *current = Value::Array(set.into_iter().collect());
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct JsonLdInstances {
    #[serde(rename = "@context")]
    context: serde_json::Map<String, serde_json::Value>,
    #[serde(rename = "@graph")]
    graph: Vec<JsonLdInstance>,
}

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

    fn create_context(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut context = serde_json::Map::new();

        // Add standard prefixes
        context.insert(
            "xsd".to_string(),
            serde_json::Value::String("http://www.w3.org/2001/XMLSchema#".to_string()),
        );

        // Add model baseIRI for term resolution
        context.insert(
            "@vocab".to_string(),
            serde_json::Value::String(self.manifest.model.base_iri.clone()),
        );

        // Add instance baseIRI for reference resolution
        context.insert(
            "@base".to_string(),
            serde_json::Value::String(self.manifest.instances.base_iri.clone()),
        );

        // Add property mappings from vocabulary
        if let Some(vocab) = &self.vocabulary {
            for prop in vocab.properties.values() {
                let mut property_context = serde_json::Map::new();
                property_context.insert(
                    "@id".to_string(),
                    serde_json::Value::String(prop.id.clone()),
                );

                // If range is a class (not xsd:*), mark as @id type
                if let Some(range) = &prop.range {
                    if range.iter().any(|r| !r.starts_with("xsd:")) {
                        property_context.insert(
                            "@type".to_string(),
                            serde_json::Value::String("@id".to_string()),
                        );
                    } else if range.iter().any(|r| r.starts_with("xsd:")) {
                        property_context.insert(
                            "@type".to_string(),
                            serde_json::Value::String(range[0].clone()),
                        );
                    }
                }

                context.insert(
                    prop.label.clone(),
                    serde_json::Value::Object(property_context),
                );
            }
        }

        context
    }

    // fn is_reference_property(&self, property_label: &str) -> bool {
    //     if let Some(vocab) = &self.vocabulary {
    //         for (_, prop) in &vocab.properties {
    //             if prop.label == property_label {
    //                 if let Some(range) = &prop.range {
    //                     return range.iter().any(|r| !r.starts_with("xsd:"));
    //                 }
    //             }
    //         }
    //     }
    //     false
    // }

    fn get_valid_property_labels(&self, class_type: &str) -> HashSet<String> {
        let mut valid_labels = HashSet::new();
        let vocab = self.vocabulary.as_ref().unwrap();
        let class_iri = format!("{}{}", self.manifest.model.base_iri, class_type);

        // Get properties from class's rdfs:range
        if let Some(class) = vocab.classes.values().find(|c| c.id == class_iri) {
            if let Some(props) = &class.range {
                for prop_iri in props {
                    // Get property label from vocabulary
                    if let Some(prop) = vocab.properties.values().find(|p| p.id == *prop_iri) {
                        valid_labels.insert(prop.label.clone());
                    }
                }
            }
        }

        tracing::debug!(
            "Valid labels for class '{}': {:?}",
            class_type,
            valid_labels
        );

        // Get properties that have this class in their rdfs:domain
        for prop in vocab.properties.values() {
            if let Some(domains) = &prop.domain {
                if domains.contains(&class_iri) {
                    valid_labels.insert(prop.label.clone());
                }
            }
        }

        tracing::debug!(
            "Valid labels for class '{}' after domain check: {:?}",
            class_type,
            valid_labels
        );

        // TODO: When rdfs:subClassOf is implemented, recursively get properties from parent classes
        // This would involve:
        // 1. Finding parent class from rdfs:subClassOf property
        // 2. Recursively getting properties from parent class's range and domain

        valid_labels
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

        // Get the identifier property for this class
        let identifier_label = vocab.get_identifier_label(&class_type).ok_or_else(|| {
            ProcessorError::Processing(format!(
                "No identifier property found for class '{}'. To import instances of this class, your data model CSVs must indicate which column of the instance data will be used as the identifier (\"@id\") for instances of this class.",
                class_type
            ))
        })?;

        let file_path = PathBuf::from(instance_path).join(&step.path);
        tracing::debug!("Reading instance data from {:?}", file_path);

        let mut rdr = csv::Reader::from_path(&file_path)
            .map_err(|e| ProcessorError::Processing(format!("Failed to read CSV: {}", e)))?;

        // Read headers first and collect them into a Vec
        let mut headers: Vec<String> = rdr
            .headers()
            .map_err(|e| ProcessorError::Processing(format!("Failed to read CSV headers: {}", e)))?
            .iter()
            .map(|h| h.to_string())
            .collect();

        tracing::debug!("CSV headers: {:?}", headers);

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

        tracing::debug!("Filtered headers: {:?}", headers);

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
                        // if self.is_reference_property(header) {
                        //     // For reference properties, create an @id object
                        //     let mut ref_obj = serde_json::Map::new();
                        //     ref_obj.insert(
                        //         "@id".to_string(),
                        //         serde_json::Value::String(format!(
                        //             "{}{}",
                        //             self.manifest.instances.base_iri, value
                        //         )),
                        //     );
                        //     properties
                        //         .insert(header.to_string(), serde_json::Value::Object(ref_obj));
                        // } else {
                        // Handle numeric values
                        if value.contains('$') || value.contains('%') {
                            let cleaned_value = value.replace(['$', '%', ','], "");
                            if let Ok(num) = cleaned_value.parse::<f64>() {
                                properties.insert(
                                    header.to_string(),
                                    serde_json::Value::Number(
                                        serde_json::Number::from_f64(num).unwrap(),
                                    ),
                                );
                                continue;
                            }
                        }
                        // Default to string values
                        properties.insert(
                            header.to_string(),
                            serde_json::Value::String(value.to_string()),
                        );
                        // }
                    }
                }
            }

            let instance = JsonLdInstance {
                id: id.to_string(),
                type_: vec![class_type.clone()],
                properties,
            };

            // Update or insert instance
            match self.instances.entry(id.to_string()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(instance);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().update_with(instance)?;
                }
            }
        }

        Ok(())
    }

    pub async fn save_instances(&self, output_path: &PathBuf) -> Result<(), ProcessorError> {
        let instances = JsonLdInstances {
            context: self.create_context(),
            graph: self.instances.values().cloned().collect(),
        };

        let instances_json = serde_json::to_string_pretty(&instances).map_err(|e| {
            ProcessorError::Processing(format!("Failed to serialize instances: {}", e))
        })?;

        let output_dir = if output_path.is_dir() {
            output_path.clone()
        } else {
            // If the path doesn't exist yet, assume it's intended to be a directory
            Path::new(&output_path).to_path_buf()
        };

        // Create the full file path: output_dir/vocabulary.jsonld
        let output_path = output_dir.join("instances.jsonld");

        // Ensure the directory exists
        fs::create_dir_all(&output_dir).map_err(|e| {
            ProcessorError::Processing(format!(
                "Failed to create directory for instances file: {}",
                e
            ))
        })?;

        // Write the JSON to the file
        fs::write(&output_path, instances_json).map_err(|e| {
            ProcessorError::Processing(format!("Failed to write instances file: {}", e))
        })?;

        let output_path_str = output_path.to_string_lossy();
        tracing::info!("Saved instances to {}", output_path_str);

        Ok(())
    }
}
