use crate::contains_variant;
use crate::error::ProcessorError;
use crate::types::{ColumnOverride, ExtraItem, PivotColumn};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum ModelStep {
    BasicVocabularyStep,
    SubClassVocabularyStep,
    PropertiesVocabularyStep,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum InstanceStep {
    BasicInstanceStep,
    SubClassInstanceStep,
    PropertiesInstanceStep,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum StepType {
    CSVImportStep,
    ModelStep(ModelStep),
    InstanceStep(InstanceStep),
}

impl<'de> Deserialize<'de> for StepType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StepTypeVisitor;

        impl<'de> Visitor<'de> for StepTypeVisitor {
            type Value = StepType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a valid StepType string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "CSVImportStep" => Ok(StepType::CSVImportStep),
                    "BasicVocabularyStep" => {
                        Ok(StepType::ModelStep(ModelStep::BasicVocabularyStep))
                    }
                    "SubClassVocabularyStep" => {
                        Ok(StepType::ModelStep(ModelStep::SubClassVocabularyStep))
                    }
                    "PropertiesVocabularyStep" => {
                        Ok(StepType::ModelStep(ModelStep::PropertiesVocabularyStep))
                    }
                    "BasicInstanceStep" => {
                        Ok(StepType::InstanceStep(InstanceStep::BasicInstanceStep))
                    }
                    "SubClassInstanceStep" => {
                        Ok(StepType::InstanceStep(InstanceStep::SubClassInstanceStep))
                    }
                    "PropertiesInstanceStep" => {
                        Ok(StepType::InstanceStep(InstanceStep::PropertiesInstanceStep))
                    }
                    _ => Err(de::Error::unknown_variant(
                        value,
                        &[
                            "CSVImportStep",
                            "BasicVocabularyStep",
                            "SubClassVocabularyStep",
                            "BasicInstanceStep",
                            "SubClassInstanceStep",
                            "PropertiesInstanceStep",
                        ],
                    )),
                }
            }
        }

        deserializer.deserialize_str(StepTypeVisitor)
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ImportStep {
    #[serde(default)]
    pub path: String,
    #[serde(rename = "@type")]
    pub types: Vec<StepType>,
    #[serde(default)]
    pub overrides: Vec<ColumnOverride>,
    #[serde(default, rename = "extraItems")]
    pub extra_items: Vec<ExtraItem>,
    #[serde(default, rename = "instanceType")]
    pub instance_type: String,
    pub ignore: Option<Vec<String>>,
    #[serde(rename = "replaceClassIdWith")]
    pub replace_class_id_with: Option<String>,
    #[serde(rename = "replacePropertyIdWith")]
    pub replace_property_id_with: Option<String>,
    // Required if the types include SubClassVocabularyStep
    #[serde(rename = "subClassOf")]
    pub sub_class_of: Option<Vec<String>>,
    // Required if the types include SubClassInstanceStep
    #[serde(rename = "subClassProperty")]
    pub sub_class_property: Option<String>,
    #[serde(rename = "pivotColumns")]
    pub pivot_columns: Option<Vec<PivotColumn>>,
    #[serde(default, rename = "namespaceIris")]
    pub namespace_iris: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportSection {
    #[serde(default, rename = "baseIRI")]
    pub base_iri: String,
    #[serde(default)]
    pub path: String,
    pub sequence: Vec<ImportStep>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
            let model_step: Vec<&StepType> = step
                .types
                .iter()
                .filter(|t| matches!(t, StepType::ModelStep(_)))
                .collect();

            if model_step.is_empty() {
                tracing::error!("No valid model step type found: {:?}", step.types);
                return Err(ProcessorError::InvalidManifest(
                    "Model sequence steps must include ModelStep type: BasicVocabularyStep or SubClassVocabularyStep".into(),
                ));
            }

            if model_step.len() > 1 {
                tracing::error!("Multiple model step types found: {:?}", step.types);
                return Err(ProcessorError::InvalidManifest(
                    "Model sequence steps must include only one ModelStep type: BasicVocabularyStep or SubClassVocabularyStep".into(),
                ));
            }

            if let StepType::ModelStep(ModelStep::SubClassVocabularyStep) = model_step[0] {
                if step.sub_class_of.is_none() {
                    tracing::error!("SubClassVocabularyStep requires subClassOf field");
                    return Err(ProcessorError::InvalidManifest(
                        "SubClassVocabularyStep requires subClassOf field".into(),
                    ));
                }
            }
        }

        for step in &self.instances.sequence {
            let instance_steps: Vec<&StepType> = step
                .types
                .iter()
                .filter(|t| matches!(t, StepType::InstanceStep(_)))
                .collect();

            if !contains_variant!(instance_steps, StepType::InstanceStep(_)) {
                tracing::error!("Invalid instance step type: {:?}", step.types);
                return Err(ProcessorError::InvalidManifest(
                    "Instance sequence steps must include InstanceStep type".into(),
                ));
            }

            if instance_steps.is_empty() {
                tracing::error!("No valid instance step type found: {:?}", step.types);
                return Err(ProcessorError::InvalidManifest(
                    "Instance sequence steps must include InstanceStep type: BasicInstanceStep, SubClassInstanceStep, or PropertiesInstanceStep".into(),
                ));
            }

            if instance_steps.len() > 1 {
                tracing::error!("Multiple instance step types found: {:?}", step.types);
                return Err(ProcessorError::InvalidManifest(
                    "Instance sequence steps must include only one InstanceStep type: BasicInstanceStep, SubClassInstanceStep, or PropertiesInstanceStep".into(),
                ));
            }

            // Validate SubClassInstanceStep has required subClassProperty
            if let StepType::InstanceStep(InstanceStep::SubClassInstanceStep) = instance_steps[0] {
                if step.sub_class_property.is_none() {
                    tracing::error!("SubClassInstanceStep requires subClassProperty field");
                    return Err(ProcessorError::InvalidManifest(
                        "SubClassInstanceStep requires subClassProperty field".into(),
                    ));
                }
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
