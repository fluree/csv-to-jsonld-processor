use crate::error::ProcessorError;
use crate::types::{ColumnOverride, ExtraItem};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ImportStep {
    pub path: String,
    #[serde(rename = "@type")]
    pub types: Vec<String>,
    #[serde(default)]
    pub overrides: Vec<ColumnOverride>,
    #[serde(default, rename = "extraItems")]
    pub extra_items: Vec<ExtraItem>,
    #[serde(default, rename = "instanceType")]
    pub instance_type: String,
    pub ignore: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImportSection {
    #[serde(rename = "baseIRI")]
    pub base_iri: String,
    pub path: String,
    pub sequence: Vec<ImportStep>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@context")]
    pub context: serde_json::Value,
    #[serde(rename = "@type")]
    pub type_: String,
    pub name: String,
    pub description: String,
    pub model: ImportSection,
    pub instances: ImportSection,
}

impl Manifest {
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<Self, ProcessorError> {
        let path = path.into();
        tracing::info!("Loading manifest from {:?}", path);
        let file = std::fs::File::open(&path)?;
        let manifest = serde_json::from_reader(file)?;
        tracing::info!("Successfully loaded manifest: {}", path.display());
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), ProcessorError> {
        tracing::info!("Validating manifest...");

        if self.type_ != "CSVImportManifest" {
            tracing::error!("Invalid manifest type: {}", self.type_);
            return Err(ProcessorError::InvalidManifest(
                "Manifest must have @type of CSVImportManifest".into(),
            ));
        }

        for step in &self.model.sequence {
            if !step.types.contains(&"ModelStep".to_string()) {
                tracing::error!("Invalid model step type: {:?}", step.types);
                return Err(ProcessorError::InvalidManifest(
                    "Model sequence steps must include ModelStep type".into(),
                ));
            }
        }

        for step in &self.instances.sequence {
            if !step.types.contains(&"InstanceStep".to_string()) {
                tracing::error!("Invalid instance step type: {:?}", step.types);
                return Err(ProcessorError::InvalidManifest(
                    "Instance sequence steps must include InstanceStep type".into(),
                ));
            }
        }

        tracing::info!("Manifest validation successful");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_loading() {
        let manifest = Manifest::from_file("../test-data/manifest.jsonld").unwrap();
        assert_eq!(manifest.type_, "CSVImportManifest");
        assert!(manifest.validate().is_ok());
    }
}
