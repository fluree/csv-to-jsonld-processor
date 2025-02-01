use crate::contains_variant;
use crate::error::{ProcessingState, ProcessorError};
use crate::types::{ColumnOverride, ExtraItem, PivotColumn};
use csv::StringRecord;
use json_comments::StripComments;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashSet;
use std::fmt::Display;
use std::io::{self, Cursor, Read, Seek};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::{fmt, fs, mem};
use tokio::io::AsyncRead;

pub trait ReadSeek: Read + Seek + Send {}
impl<T: Read + Seek + Send> ReadSeek for T {}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StorageLocation {
    Local(PathBuf),
    S3 { bucket: String, key: PathBuf },
}

impl Default for StorageLocation {
    fn default() -> Self {
        StorageLocation::Local(PathBuf::new())
    }
}

impl Display for StorageLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageLocation::Local(path) => write!(f, "{}", path.display()),
            StorageLocation::S3 { bucket, key } => write!(f, "s3://{}/{}", bucket, key.display()),
        }
    }
}

impl<'de> Deserialize<'de> for StorageLocation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        StorageLocation::try_from(s).map_err(serde::de::Error::custom)
    }
}

impl Serialize for StorageLocation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            StorageLocation::Local(path) => serializer.serialize_str(&path.to_string_lossy()),
            StorageLocation::S3 { bucket, key } => {
                serializer.serialize_str(&format!("s3://{}/{}", bucket, key.to_string_lossy()))
            }
        }
    }
}

impl FromStr for StorageLocation {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok((bucket, key)) = StorageLocation::parse_s3_uri(s) {
            Ok(StorageLocation::S3 { bucket, key })
        } else {
            Ok(StorageLocation::Local(PathBuf::from(s)))
        }
    }
}

impl TryFrom<String> for StorageLocation {
    type Error = ProcessorError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if let Ok((bucket, key)) = StorageLocation::parse_s3_uri(&value) {
            Ok(StorageLocation::S3 { bucket, key })
        } else {
            match PathBuf::from_str(&value) {
                Ok(path) => Ok(StorageLocation::Local(path)),
                Err(_) => Err(ProcessorError::InvalidManifest(
                    "Invalid storage location".into(),
                )),
            }
        }
    }
}

impl StorageLocation {
    pub async fn read_contents(
        &self,
        s3_client: Option<&aws_sdk_s3::Client>,
    ) -> io::Result<Vec<u8>> {
        match self {
            StorageLocation::Local(path) => fs::read(path),
            StorageLocation::S3 { bucket, key } => {
                let s3_client = s3_client.expect("S3 client is required for S3 operations");

                let resp = s3_client
                    .get_object()
                    .bucket(bucket)
                    .key(key.to_str().unwrap())
                    .send()
                    .await
                    .map_err(|e| {
                        io::Error::new(io::ErrorKind::Other, format!("S3 read error: {}", e))
                    })?;

                let body = resp.body.collect().await.map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("S3 body read error: {}", e))
                })?;
                Ok(body.into_bytes().to_vec())
            }
        }
    }

    pub fn is_dir(&self) -> bool {
        match self {
            StorageLocation::Local(path) => path.is_dir(),
            // TODO: This isn't great, but the only other option is to make an S3 request to list
            StorageLocation::S3 { key, .. } => key.to_string_lossy().ends_with('/'),
        }
    }

    pub fn join(&self, path: &str) -> StorageLocation {
        match self {
            StorageLocation::Local(base) => StorageLocation::Local(base.join(path)),
            StorageLocation::S3 { bucket, key } => {
                let new_key = key.join(path);
                StorageLocation::S3 {
                    bucket: bucket.clone(),
                    key: new_key,
                }
            }
        }
    }

    pub async fn get_reader(
        &self,
        s3_client: Option<&aws_sdk_s3::Client>,
    ) -> io::Result<Box<dyn ReadSeek>> {
        match self {
            StorageLocation::Local(path) => {
                let file = fs::File::open(path)?;
                Ok(Box::new(file))
            }
            StorageLocation::S3 { .. } => {
                // For S3, we still need to download the content since we can't stream it directly
                // But we wrap it in a Cursor to provide a Read interface
                let contents = self.read_contents(s3_client).await?;
                Ok(Box::new(Cursor::new(contents)))
            }
        }
    }

    pub async fn write_contents<P: AsRef<[u8]>>(
        &self,
        instances: &P,
        s3_client: Option<&aws_sdk_s3::Client>,
    ) -> Result<(), ProcessorError> {
        match self {
            StorageLocation::Local(output_path) => {
                if let Some(parent) = output_path.parent() {
                    fs::create_dir_all(parent).map_err(|e| {
                        ProcessorError::Processing(format!(
                            "Failed to create directory for instances file: {}",
                            e
                        ))
                    })?;
                }

                fs::write(output_path, instances).map_err(|e| {
                    ProcessorError::Processing(format!(
                        "Failed to write file @ {}: {}",
                        &output_path.to_string_lossy(),
                        e
                    ))
                })?;

                let output_path_str = output_path.to_string_lossy();
                tracing::info!("Saved instances to {}", output_path_str);
            }
            StorageLocation::S3 { bucket, key } => {
                let s3_client = s3_client.ok_or(ProcessorError::InvalidManifest(
                    "S3 client is required for S3 operations".into(),
                ))?;

                let instances: Vec<u8> = instances.as_ref().to_vec();

                s3_client
                    .put_object()
                    .bucket(bucket)
                    .key(key.to_str().unwrap())
                    .body(instances.into())
                    .send()
                    .await
                    .expect("Failed to write to S3");
            }
        }
        Ok(())
    }

    fn file_name(&self) -> String {
        match self {
            StorageLocation::Local(key) | StorageLocation::S3 { key, .. } => key
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        }
    }

    pub fn file_stem(&self) -> Option<String> {
        match self {
            StorageLocation::Local(key) | StorageLocation::S3 { key, .. } => {
                key.file_stem().map(|s| s.to_string_lossy().to_string())
            }
        }
    }

    fn parse_s3_uri(uri: &str) -> io::Result<(String, PathBuf)> {
        if !uri.starts_with("s3://") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid S3 URI",
            ));
        }
        let without_scheme = &uri[5..];
        let parts: Vec<&str> = without_scheme.splitn(2, '/').collect();

        if parts.len() != 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "S3 URI must be in the format s3://bucket/key",
            ));
        }

        let key_as_path_buf = PathBuf::from(parts[1]);

        Ok((parts[0].to_string(), key_as_path_buf))
    }
}

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

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct ImportStep {
    #[serde(default)]
    pub path: StorageLocation,
    #[serde(default)]
    pub sheet: Option<String>,
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
    #[serde(rename = "subClassOf")]
    pub sub_class_of: Option<Vec<String>>,
    #[serde(rename = "subClassProperty")]
    pub sub_class_property: Option<String>,
    #[serde(rename = "pivotColumns")]
    pub pivot_columns: Option<Vec<PivotColumn>>,
    #[serde(rename = "delimitValuesOn")]
    pub delimit_values_on: Option<String>,
    #[serde(rename = "mapToLabel")]
    pub map_to_label: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ImportSection {
    #[serde(default, rename = "baseIRI")]
    pub base_iri: String,
    #[serde(default, rename = "namespaceIris")]
    pub namespace_iris: bool,
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

#[derive(Debug, Deserialize, Default)]
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
    #[serde(default)]
    pub excel_file: Option<StorageLocation>,
    #[serde(default)]
    pub model: ImportSection,
    #[serde(default)]
    pub instances: ImportSection,
}

fn handle_step_deduplication(
    section: &mut ImportSection,
    section_type: &str,
    is_strict: bool,
) -> ProcessingState {
    let mut state = ProcessingState::new();
    if let Err(duplicate_steps) = &mut section.deduplicate_steps() {
        let message = format!(
            "Duplicate {} steps found for paths: {:?}",
            section_type,
            duplicate_steps
                .iter()
                .map(|s| &s.path)
                .collect::<Vec<&StorageLocation>>()
        );
        let error = ProcessorError::InvalidManifest(message.clone());
        if is_strict {
            state.add_error_from(error);
        } else {
            state.add_warning_from(error);
        }
        tracing::warn!(message);
    };
    state
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

    pub fn validate(&mut self, is_strict: bool) -> Result<ProcessingState, ProcessingState> {
        let mut state = ProcessingState::new();
        tracing::info!("Validating manifest...");

        if self.type_ != "CSVImportManifest" && self.type_ != "ExcelImportManifest" {
            tracing::error!("Invalid manifest type: {}", self.type_);
            state.add_error_from(ProcessorError::InvalidManifest(
                "Manifest must have @type of CSVImportManifest or ExcelImportManifest".into(),
            ));
        }

        if self.type_ == "ExcelImportManifest" && self.excel_file.is_none() {
            state.add_error_from(ProcessorError::InvalidManifest(
                "ExcelImportManifest requires excel_file to be specified".into(),
            ));
        }

        state.merge(handle_step_deduplication(
            &mut self.model,
            "model",
            is_strict,
        ));
        state.merge(handle_step_deduplication(
            &mut self.instances,
            "instance",
            is_strict,
        ));

        for step in &self.model.sequence {
            let model_step_types: Vec<&StepType> = step
                .types
                .iter()
                .filter(|t| matches!(t, StepType::ModelStep(_)))
                .collect();

            if model_step_types.is_empty() {
                tracing::error!("No valid model step type found: {:?}", step.types);
                state.add_error_from(ProcessorError::InvalidManifest(
                    "Model sequence steps must include ModelStep type: BasicVocabularyStep or SubClassVocabularyStep".into(),
                ));
            }

            if model_step_types.len() > 1 {
                tracing::error!("Multiple model step types found: {:?}", step.types);
                state.add_error_from(ProcessorError::InvalidManifest(
                    "Model sequence steps must include only one ModelStep type: BasicVocabularyStep or SubClassVocabularyStep".into(),
                ));
            }

            if let StepType::ModelStep(ModelStep::SubClassVocabularyStep) = model_step_types[0] {
                if step.sub_class_of.is_none() {
                    tracing::error!("SubClassVocabularyStep requires subClassOf field");
                    state.add_error_from(ProcessorError::InvalidManifest(
                        "SubClassVocabularyStep requires subClassOf field".into(),
                    ));
                }
            }

            // Validate Excel sheet reference if needed
            if self.type_ == "ExcelImportManifest" && step.sheet.is_none() {
                state.add_error_from(ProcessorError::InvalidManifest(format!(
                    "Excel manifest step missing sheet reference: {:?}",
                    step
                )));
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
                state.add_error_from(ProcessorError::InvalidManifest(
                    "Cannot have both delimitValuesOn and pivotColumns in the same step".into(),
                ));
            }

            if !contains_variant!(instance_steps, StepType::InstanceStep(_)) {
                tracing::error!("Invalid instance step type: {:?}", step.types);
                state.add_error_from(ProcessorError::InvalidManifest(
                    "Instance sequence steps must include InstanceStep type".into(),
                ));
            }

            if instance_steps.is_empty() {
                tracing::error!("No valid instance step type found: {:?}", step.types);
                state.add_error_from(ProcessorError::InvalidManifest(
                    "Instance sequence steps must include InstanceStep type: BasicInstanceStep, SubClassInstanceStep, or PropertiesInstanceStep".into(),
                ));
            }

            if instance_steps.len() > 1 {
                tracing::error!("Multiple instance step types found: {:?}", step.types);
                state.add_error_from(ProcessorError::InvalidManifest(
                    "Instance sequence steps must include only one InstanceStep type: BasicInstanceStep, SubClassInstanceStep, or PropertiesInstanceStep".into(),
                ));
            }

            if let StepType::InstanceStep(InstanceStep::SubClassInstanceStep) = instance_steps[0] {
                if step.sub_class_property.is_none() {
                    tracing::error!("SubClassInstanceStep requires subClassProperty field");
                    state.add_error_from(ProcessorError::InvalidManifest(
                        "SubClassInstanceStep requires subClassProperty field".into(),
                    ));
                }
            }

            // Validate Excel sheet reference if needed
            if self.type_ == "ExcelImportManifest" && step.sheet.is_none() {
                state.add_error_from(ProcessorError::InvalidManifest(format!(
                    "Excel manifest step missing sheet reference: {:?}",
                    step
                )));
            }
        }

        tracing::info!("Manifest validation successful");
        match state.is_ok() {
            true => Ok(state),
            false => Err(state),
        }
    }

    pub fn is_model_file(headers: &StringRecord) -> bool {
        // if every single header is contained in the list: ["Class ID", "Class Name", "Property ID", "Property Name", "Property Description", "Type", "Class Range"]
        let model_headers = [
            "Class ID",
            "Class Name",
            "Class Description",
            "Property ID",
            "Property Name",
            "Property Description",
            "Type",
            "Class Range",
        ];
        headers.iter().all(|h| model_headers.contains(&h))
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
