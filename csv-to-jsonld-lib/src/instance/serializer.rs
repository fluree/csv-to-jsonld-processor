use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Map;

use crate::error::ProcessorError;
use crate::types::{IdOpt, JsonLdInstance, JsonLdInstances, PropertyDatatype};
use crate::{Manifest, VocabularyMap};

pub struct InstanceSerializer {
    manifest: Arc<Manifest>,
}

impl InstanceSerializer {
    pub fn new(manifest: Arc<Manifest>) -> Self {
        Self { manifest }
    }

    fn create_context(&self, vocabulary: &VocabularyMap) -> Map<String, serde_json::Value> {
        let mut context = Map::new();

        // Add standard prefixes
        context.insert(
            "xsd".to_string(),
            serde_json::Value::String("http://www.w3.org/2001/XMLSchema#".to_string()),
        );

        // Add model baseIRI for term resolution
        if !self.manifest.model.base_iri.is_empty() {
            context.insert(
                "@vocab".to_string(),
                serde_json::Value::String(self.manifest.model.base_iri.clone()),
            );
        }

        // Add instance baseIRI for reference resolution
        if !self.manifest.instances.base_iri.is_empty() {
            context.insert(
                "@base".to_string(),
                serde_json::Value::String(self.manifest.instances.base_iri.clone()),
            );
        }

        // Add property mappings from vocabulary
        for prop in vocabulary.properties.values() {
            let label = if let Some(label) = &prop.label {
                label.clone()
            } else {
                continue;
            };
            let mut property_context = serde_json::Map::new();
            let property_iri = match &prop.id {
                IdOpt::String(ref iri) => iri.clone(),
                IdOpt::ReplacementMap { replacement_id, .. } => replacement_id.clone(),
            };
            property_context.insert("@id".to_string(), serde_json::Value::String(property_iri));

            // If range is a class (not xsd:*), mark as @id type
            if let Some(range) = &prop.range {
                if range.iter().any(|r| {
                    matches!(r, PropertyDatatype::URI(_))
                        || matches!(r, PropertyDatatype::Picklist(_))
                }) {
                    property_context.insert(
                        "@type".to_string(),
                        serde_json::Value::String("@id".to_string()),
                    );
                } else if !range.is_empty() {
                    property_context.insert(
                        "@type".to_string(),
                        serde_json::to_value(range[0].clone()).unwrap(),
                    );
                }
            }

            context.insert(label, serde_json::Value::Object(property_context));
        }

        context
    }

    pub async fn save_instances(
        &self,
        instances: &HashMap<String, JsonLdInstance>,
        output_path: &PathBuf,
        vocabulary: &VocabularyMap,
    ) -> Result<(), ProcessorError> {
        let instances = JsonLdInstances {
            context: self.create_context(vocabulary),
            insert: instances.values().cloned().collect(),
            ledger: "".to_string(),
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

        // Create the full file path: output_dir/instances.jsonld
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
