use crate::contains_variant;
use crate::error::ProcessorError;
use crate::types::{ColumnOverride, ExtraItem, PivotColumn};
use json_comments::StripComments;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer};
use std::collections::HashSet;
use std::io::Read;
use std::path::PathBuf;
use std::{fmt, mem};

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
    PicklistStep,
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
                    "PicklistStep" => Ok(StepType::InstanceStep(InstanceStep::PicklistStep)),
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
                            "PicklistStep",
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
    #[serde(rename = "delimitValuesOn")]
    pub delimit_values_on: Option<String>,
    #[serde(rename = "mapToLabel")]
    pub map_to_label: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportSection {
    #[serde(default, rename = "baseIRI")]
    pub base_iri: String,
    #[serde(default, rename = "namespaceIris")]
    pub namespace_iris: bool,
    #[serde(default)]
    pub path: String,
    pub sequence: Vec<ImportStep>,
}

impl ImportSection {
    pub fn deduplicate_steps(&mut self) -> Result<(), Vec<ImportStep>> {
        let mut seen_step_paths = HashSet::new();
        let mut duplicate_steps = vec![];

        let sequence = mem::take(&mut self.sequence);

        let unique_steps: Vec<ImportStep> = sequence
            .into_iter()
            .filter_map(|step| {
                if !seen_step_paths.insert(step.path.clone()) {
                    duplicate_steps.push(step);
                    None
                } else {
                    Some(step)
                }
            })
            .collect();

        self.sequence = unique_steps;
        if !duplicate_steps.is_empty() {
            return Err(duplicate_steps);
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    #[serde(rename = "@id", default)]
    pub id: String,
    #[serde(rename = "@context", default)]
    pub context: serde_json::Value,
    #[serde(rename = "@type", default)]
    pub type_: String,
    #[serde(default)]
    pub ledger: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    // TODO: change this to Option<ImportSection>
    pub model: ImportSection,
    pub instances: ImportSection,
}

/// Check for duplicate steps in either model or instances section
fn handle_step_deduplication(
    section: &mut ImportSection,
    section_type: &str,
    is_strict: bool,
) -> Result<(), ProcessorError> {
    if let Err(duplicate_steps) = &mut section.deduplicate_steps() {
        let message = format!(
            "Duplicate {} steps found for paths: {:?}",
            section_type,
            duplicate_steps
                .iter()
                .map(|s| &s.path)
                .collect::<Vec<&String>>()
        );
        if is_strict {
            return Err(ProcessorError::InvalidManifest(message));
        }
        tracing::warn!(message);
    };
    Ok(())
}

impl Manifest {
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<Self, ProcessorError> {
        let path = path.into();
        tracing::info!("Loading manifest from {:?}", path);
        let file = std::fs::File::open(&path)?;
        let reader = std::io::BufReader::new(file);
        let mut bytes_vec = Vec::new();
        let mut stripped_reader = StripComments::new(reader);
        stripped_reader.read_to_end(&mut bytes_vec)?;
        let manifest = serde_json::from_slice(&bytes_vec)?;
        tracing::info!("Successfully loaded manifest: {}", path.display());
        Ok(manifest)
    }

    pub fn validate(&mut self, is_strict: bool) -> Result<(), ProcessorError> {
        tracing::info!("Validating manifest...");

        if self.type_ != "CSVImportManifest" {
            tracing::error!("Invalid manifest type: {}", self.type_);
            return Err(ProcessorError::InvalidManifest(
                "Manifest must have @type of CSVImportManifest".into(),
            ));
        }

        handle_step_deduplication(&mut self.model, "model", is_strict)?;
        handle_step_deduplication(&mut self.instances, "instance", is_strict)?;

        if let Err(duplicate_steps) = &mut self.instances.deduplicate_steps() {
            let message = format!(
                "Duplicate instance steps found for paths: {:?}",
                duplicate_steps
                    .iter()
                    .map(|s| &s.path)
                    .collect::<Vec<&String>>()
            );
            if is_strict {
                return Err(ProcessorError::InvalidManifest(message));
            }
            tracing::warn!(message);
        };

        for step in &self.model.sequence {
            let model_step_types: Vec<&StepType> = step
                .types
                .iter()
                .filter(|t| matches!(t, StepType::ModelStep(_)))
                .collect();

            if model_step_types.is_empty() {
                tracing::error!("No valid model step type found: {:?}", step.types);
                return Err(ProcessorError::InvalidManifest(
                    "Model sequence steps must include ModelStep type: BasicVocabularyStep or SubClassVocabularyStep".into(),
                ));
            }

            if model_step_types.len() > 1 {
                tracing::error!("Multiple model step types found: {:?}", step.types);
                return Err(ProcessorError::InvalidManifest(
                    "Model sequence steps must include only one ModelStep type: BasicVocabularyStep or SubClassVocabularyStep".into(),
                ));
            }

            if let StepType::ModelStep(ModelStep::SubClassVocabularyStep) = model_step_types[0] {
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

            if step.delimit_values_on.is_some() && step.pivot_columns.is_some() {
                tracing::error!(
                    "Cannot have both delimitValuesOn and pivotColumns in the same step"
                );
                return Err(ProcessorError::InvalidManifest(
                    "Cannot have both delimitValuesOn and pivotColumns in the same step".into(),
                ));
            }

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
        let mut manifest = Manifest::from_file("../test-data/manifest.jsonld").unwrap();
        assert_eq!(manifest.type_, "CSVImportManifest");
        assert!(manifest.validate(false).is_ok());
    }
}
