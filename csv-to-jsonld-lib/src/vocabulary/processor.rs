use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::mapping::{MappingConfig, RowValues, VocabularyColumnMapping};
use crate::error::{ProcessingState, ProcessorError};
use crate::excel::ExcelReader;
use crate::manifest::{ImportStep, ModelStep, StepType, StorageLocation};
use crate::types::{
    IdOpt, OnEntity, PropertyDatatype, StrictIdOpt, StrictVocabularyMap, VocabularyMap,
    VocabularyTerm,
};
use crate::utils::{expand_iri_with_base, map_xsd_type, to_pascal_case};
use crate::{contains_variant, Manifest};

pub struct VocabularyProcessor {
    manifest: Arc<Manifest>,
    pub vocabulary: VocabularyMap,
    class_properties: HashMap<IdOpt, Vec<String>>,
    pub(crate) is_strict: bool,
    ignore: HashMap<String, Vec<String>>,
    base_iri: String,
    namespace_iris: bool,
    processing_state: ProcessingState,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VocabularyProcessorMetadata {
    pub vocabulary: StrictVocabularyMap,
    pub class_properties: Vec<(StrictIdOpt, Vec<String>)>,
    ignore: HashMap<String, Vec<String>>,
    base_iri: String,
    namespace_iris: bool,
}

impl From<&VocabularyProcessor> for VocabularyProcessorMetadata {
    fn from(value: &VocabularyProcessor) -> Self {
        let class_properties = value
            .class_properties
            .clone()
            .into_iter()
            .map(|(k, v)| (k.into(), v))
            .collect();
        let base_iri = value.manifest.model.base_iri.clone();
        let base_iri = if base_iri.is_empty() {
            value.base_iri.clone()
        } else {
            base_iri
        };
        Self {
            vocabulary: StrictVocabularyMap::from(value.vocabulary.clone()),
            class_properties,
            ignore: value.ignore.clone(),
            base_iri,
            namespace_iris: value.manifest.model.namespace_iris,
        }
    }
}

impl VocabularyProcessorMetadata {
    pub async fn from_file(
        path: &StorageLocation,
        s3_client: Option<&aws_sdk_s3::Client>,
    ) -> Result<Self, ProcessorError> {
        tracing::debug!("Loading vocabulary meta file: {:#?}", path);
        let bytes = path.read_contents(s3_client).await?;
        serde_json::from_slice(&bytes).map_err(|e| {
            ProcessorError::Processing(format!(
                "Failed to deserialize vocabulary meta file: {:#?}",
                e
            ))
        })
    }
}

impl VocabularyProcessor {
    pub fn new(manifest: Arc<Manifest>, is_strict: bool) -> Self {
        let base_iri = manifest.model.base_iri.clone();
        let namespace_iris = manifest.model.namespace_iris;
        let ignore = manifest
            .model
            .sequence
            .iter()
            .fold(HashMap::new(), |mut acc, step| {
                if let Some(ignores) = &step.ignore {
                    acc.insert(step.id(), ignores.clone());
                }
                acc
            });
        Self {
            manifest,
            vocabulary: VocabularyMap::new(),
            class_properties: HashMap::new(),
            is_strict,
            ignore,
            base_iri,
            namespace_iris,
            processing_state: ProcessingState::new(),
        }
    }

    pub fn get_base_iri(&self) -> &String {
        &self.base_iri
    }

    pub async fn new_from_vocab_meta(
        manifest: Arc<Manifest>,
        path: &StorageLocation,
        is_strict: bool,
        s3_client: Option<&aws_sdk_s3::Client>,
    ) -> Result<Self, ProcessorError> {
        let vocabulary_processor_metadata =
            VocabularyProcessorMetadata::from_file(path, s3_client).await?;
        let class_properties = vocabulary_processor_metadata
            .class_properties
            .into_iter()
            .map(|(k, v)| (k.into(), v))
            .collect();
        Ok(Self {
            manifest,
            vocabulary: vocabulary_processor_metadata.vocabulary.into(),
            class_properties,
            is_strict,
            ignore: vocabulary_processor_metadata.ignore,
            base_iri: vocabulary_processor_metadata.base_iri,
            namespace_iris: vocabulary_processor_metadata.namespace_iris,
            processing_state: ProcessingState::new(),
        })
    }

    pub fn mapping_config_from_headers(
        &mut self,
        headers: &csv::StringRecord,
        mut step: ImportStep,
        is_strict: bool,
    ) -> Result<MappingConfig, ProcessorError> {
        // Default column names
        let mut mapping = if contains_variant!(
            &step.types,
            StepType::ModelStep(ModelStep::BasicVocabularyStep)
        ) {
            MappingConfig::new(
                StepType::ModelStep(ModelStep::BasicVocabularyStep),
                VocabularyColumnMapping::basic_vocabulary_step(),
                is_strict,
            )
        } else if contains_variant!(
            &step.types,
            StepType::ModelStep(ModelStep::SubClassVocabularyStep)
        ) {
            MappingConfig::new(
                StepType::ModelStep(ModelStep::SubClassVocabularyStep),
                VocabularyColumnMapping::sub_class_vocabulary_step(),
                is_strict,
            )
        } else if contains_variant!(
            &step.types,
            StepType::ModelStep(ModelStep::PropertiesVocabularyStep)
        ) {
            MappingConfig::new(
                StepType::ModelStep(ModelStep::PropertiesVocabularyStep),
                VocabularyColumnMapping::property_vocabulary_step(),
                is_strict,
            )
        } else {
            let msg = format!("Invalid step type: {:#?}", step);
            if is_strict {
                tracing::error!("Step Error: {:#?}", step);
                return Err(ProcessorError::Processing(msg));
            } else {
                tracing::warn!("{}, using basic vocabulary step", msg);
                MappingConfig::new(
                    StepType::ModelStep(ModelStep::BasicVocabularyStep),
                    VocabularyColumnMapping::basic_vocabulary_step(),
                    is_strict,
                )
            }
        };

        // Apply any overrides from the manifest
        for override_ in &step.overrides {
            let mapping_type = &mapping.type_;
            mapping
                .column_mapping
                .handle_override(override_, mapping_type)?;
        }

        if let Some(replace_class_id_with) = &step.replace_class_id_with {
            mapping
                .column_mapping
                .replace_class_id_with(replace_class_id_with)?;
        }

        if matches!(
            mapping.column_mapping.class_column,
            IdOpt::ReplacementMap { .. }
        ) {
            tracing::debug!("Class column is a replacement map, replacing with class_id");
            tracing::debug!("Class column: {:#?}", mapping.column_mapping.class_column);
        }

        if let Some(replace_property_id_with) = &step.replace_property_id_with {
            mapping
                .column_mapping
                .replace_property_id_with(replace_property_id_with)?;
        }

        for extra_item in step.extra_items.drain(..) {
            mapping
                .column_mapping
                .extra_items
                .insert(extra_item.column.clone(), extra_item);
        }

        // Validate headers
        let column_mapping_process_state = mapping
            .column_mapping
            .validate_headers(headers, is_strict)?;

        self.processing_state.merge(column_mapping_process_state);

        Ok(mapping)
    }

    pub async fn process_vocabulary(
        &mut self,
        step: ImportStep,
        s3_client: Option<&aws_sdk_s3::Client>,
    ) -> Result<ProcessingState, ProcessorError> {
        let step_id = step.id().clone();
        tracing::debug!("Reading vocabulary data from {:?}", step_id);

        let csv_bytes = if let Some(sheet_name) = &step.sheet {
            // Excel processing
            let excel_file = self.manifest.excel_file.as_ref().ok_or_else(|| {
                ProcessorError::Processing("Excel file not specified in manifest".into())
            })?;
            let reader = excel_file.get_reader(s3_client).await.map_err(|e| {
                tracing::error!("Failed to get Excel reader for {:#?}: {}", &excel_file, e);
                ProcessorError::Processing(format!("Failed to get Excel reader: {}", e))
            })?;

            let mut excel_reader = ExcelReader::new(reader)?;
            excel_reader.get_sheet_as_csv(sheet_name)?
        } else {
            // CSV processing
            step.path.read_contents(s3_client).await.map_err(|e| {
                ProcessorError::Processing(format!("Failed to read CSV @ {}: {}", &step_id, e))
            })?
        };

        let mut rdr = csv::Reader::from_reader(csv_bytes.as_slice());

        // Get headers and build column mapping
        let headers = rdr.headers().map_err(|e| {
            ProcessorError::Processing(format!(
                "Failed to read CSV headers in file / sheet, {}: {}",
                &step_id, e
            ))
        })?;

        // TODO: This is bad... we need to think about a scenario like an excel spreadsheet where we can't know if each sheet is a model file or instance file
        if !Manifest::is_model_file(headers.iter().collect()) {
            tracing::warn!(
                "CSV or sheet {} does not appear to be a model file, skipping",
                step_id
            );
            return Ok(take(&mut self.processing_state));
        }

        tracing::debug!("Headers: {:#?}", headers);

        let sub_class_of = step.sub_class_of.clone();

        let mut mapping = self.mapping_config_from_headers(headers, step, self.is_strict)?;

        let ignorable_headers = self.ignore.get(&step_id);

        let headers = match ignorable_headers {
            Some(ignorable_headers) => headers
                .iter()
                .map(|h| {
                    if !ignorable_headers.contains(&h.to_string()) {
                        h
                    } else {
                        ""
                    }
                })
                .collect(),
            None => headers.clone(),
        };

        tracing::debug!("Filtered headers: {:?}", headers);

        // Process each row
        for (row, result) in rdr.records().enumerate() {
            let record = match result {
                Ok(record) => record,
                Err(e) => {
                    let msg = format!("Failed to read CSV record in row {}: {}", row + 1, e);
                    if self.is_strict {
                        self.processing_state
                            .add_error_from(ProcessorError::Processing(msg));
                        continue;
                    } else {
                        self.processing_state.add_warning(
                            format!("{}, skipping row", msg),
                            Some("vocabulary_processing".to_string()),
                        );
                        continue;
                    }
                }
            };

            let row_values = match mapping.extract_values(&record, &headers) {
                Ok(row_values) => row_values,
                Err(e) => {
                    if self.is_strict {
                        self.processing_state.add_error_from(e);
                        continue;
                    } else {
                        self.processing_state.add_warning_from(e);
                        continue;
                    }
                }
            };

            match self.process_class_term(&row_values, sub_class_of.clone()) {
                Ok(_) => (),
                Err(e) => {
                    if self.is_strict {
                        self.processing_state.add_error_from(e);
                        continue;
                    } else {
                        self.processing_state.add_warning_from(e);
                        continue;
                    }
                }
            }
            if !matches!(
                &mapping.type_,
                StepType::ModelStep(ModelStep::SubClassVocabularyStep)
            ) {
                if let Err(e) = self.process_property_term(&row_values) {
                    if self.is_strict {
                        self.processing_state.add_error_from(e);
                        continue;
                    } else {
                        self.processing_state.add_warning_from(e);
                        continue;
                    }
                }
            }
        }

        let picklist_classes = self
            .vocabulary
            .properties
            .values()
            .filter_map(|p| {
                if let Some(PropertyDatatype::Picklist(class)) = p.range.as_ref()?.first() {
                    class.as_ref().map(|string| {
                        IdOpt::String(string.clone()).without_base_iri(&self.base_iri)
                    })
                } else {
                    None
                }
            })
            .collect::<HashSet<IdOpt>>();

        let picklist_classes = self
            .class_properties
            .keys()
            .filter_map(|k| {
                if picklist_classes.contains(&k.normalize().to_pascal_case()) {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect::<HashSet<IdOpt>>();

        for class_id in picklist_classes {
            if let Err(e) = self.handle_add_rdfs_label_property(&class_id) {
                if self.is_strict {
                    self.processing_state.add_error_from(e);
                    continue;
                } else {
                    self.processing_state.add_warning_from(e);
                    continue;
                }
            }
        }

        // Update class terms with their properties
        for (class_id, properties) in &self.class_properties {
            if let Some(class_term) = self.vocabulary.classes.get_mut(class_id) {
                class_term.range = Some(
                    properties
                        .iter()
                        .map(|p| {
                            PropertyDatatype::URI(Some(expand_iri_with_base(&self.base_iri, p)))
                        })
                        .collect(),
                );
            }
        }

        Ok(take(&mut self.processing_state))
    }

    fn process_class_term(
        &mut self,
        row_values: &RowValues,
        sub_class_of: Option<Vec<String>>,
    ) -> Result<(), ProcessorError> {
        let RowValues {
            class_id,
            class_name,
            class_description,
            extra_items,
            ..
        } = row_values;

        let mut extra_items_result = HashMap::new();
        for extra_item in extra_items.values() {
            if matches!(extra_item.on_entity, OnEntity::Class) {
                extra_items_result.insert(
                    extra_item.map_to.clone(),
                    extra_item.value.clone().unwrap_or("".to_string()),
                );
            }
        }

        match self.vocabulary.classes.entry(class_id.clone()) {
            Entry::Vacant(_) => {
                let class_term = VocabularyTerm {
                    id: class_id
                        .normalize()
                        .to_pascal_case()
                        .with_base_iri(&self.base_iri),
                    type_: vec!["rdfs:Class".to_string()],
                    sub_class_of,
                    label: class_name.map(|n| n.to_string()),
                    comment: class_description.map(|d| d.to_string()),
                    domain: None,
                    range: Some(vec![]),
                    extra_items: extra_items_result,
                    one_of: None,
                };

                self.vocabulary.classes.insert(class_id.clone(), class_term);
            }
            Entry::Occupied(mut entry) => {
                entry.get_mut().update_with(VocabularyTerm {
                    id: class_id
                        .normalize()
                        .to_pascal_case()
                        .with_base_iri(&self.base_iri),
                    type_: vec!["rdfs:Class".to_string()],
                    sub_class_of,
                    label: class_name.map(|n| n.to_string()),
                    comment: class_description.map(|d| d.to_string()),
                    domain: None,
                    range: Some(vec![]),
                    extra_items: extra_items_result,
                    one_of: None,
                })?;
            }
        }

        Ok(())
    }

    fn process_property_term(&mut self, row_values: &RowValues) -> Result<(), ProcessorError> {
        let RowValues {
            property_id,
            property_name,
            property_description,
            property_type,
            property_class,
            class_id,
            extra_items,
            ..
        } = row_values;

        let property = property_id.as_ref().unwrap();
        let property_name = property_name.unwrap_or_default();
        let property_desc = property_description.unwrap_or_default();
        let property_type = property_type.unwrap_or("string"); // Default to string type
        let property_class = property_class.unwrap_or_default();
        let extra_items = extra_items.clone();

        let xsd_type = map_xsd_type(property_type)?;
        let camel_name = property.to_camel_case();
        let range = if !property_class.is_empty() {
            let value = match xsd_type {
                PropertyDatatype::Picklist(_) => PropertyDatatype::Picklist(Some(
                    expand_iri_with_base(&self.base_iri, &to_pascal_case(property_class)),
                )),
                PropertyDatatype::URI(_) | PropertyDatatype::ID => PropertyDatatype::URI(Some(
                    expand_iri_with_base(&self.base_iri, &to_pascal_case(property_class)),
                )),
                _ => {
                    let error_string = format!(
                        "[Property: {}] A property with type {} cannot have a target class ({})",
                        property_name, property_type, property_class
                    );
                    if self.is_strict {
                        return Err(ProcessorError::Processing(error_string));
                    } else {
                        self.processing_state
                            .add_warning(error_string, Some("property_processing".to_string()));
                        PropertyDatatype::URI(Some(expand_iri_with_base(
                            &self.base_iri,
                            &to_pascal_case(property_class),
                        )))
                    }
                }
            };
            Some(vec![value])
        } else {
            Some(vec![xsd_type.clone()])
        };

        let mut extra_items_result = HashMap::new();
        for (_, extra_item) in extra_items {
            if matches!(extra_item.on_entity, OnEntity::Property) {
                extra_items_result.insert(
                    extra_item.map_to.clone(),
                    extra_item.value.clone().unwrap_or("".to_string()),
                );
            }
        }

        let new_or_existing_class_id = self
            .vocabulary
            .classes
            .keys()
            .find(|k| k == &class_id)
            .unwrap_or(class_id);

        tracing::debug!(
            "Processing property term: {} (class: {})",
            camel_name,
            new_or_existing_class_id
        );

        // Create property term
        let property_term = VocabularyTerm {
            id: camel_name.with_base_iri(&self.base_iri),
            type_: vec!["rdf:Property".to_string()],
            sub_class_of: None,
            label: Some(property_name.to_string()),
            comment: Some(property_desc.to_string()),
            domain: Some(vec![new_or_existing_class_id
                .normalize()
                .to_pascal_case()
                .with_base_iri(&self.base_iri)
                .final_iri()]),
            range,
            extra_items: extra_items_result,
            one_of: None,
        };

        // If it's an ID property, store it in identifiers map
        if matches!(xsd_type, PropertyDatatype::ID) {
            self.vocabulary.identifiers.insert(
                class_id.normalize().to_pascal_case().to_string(),
                property_term,
            );
        } else {
            // Otherwise store it in properties map
            match self.vocabulary.properties.entry(camel_name.clone()) {
                std::collections::hash_map::Entry::Vacant(_) => {
                    self.vocabulary
                        .properties
                        .insert(camel_name.clone(), property_term);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().update_with(property_term)?
                }
            }

            let class_entry = self.class_properties.entry(class_id.clone()).or_default();

            class_entry.push(camel_name.final_iri());
        }

        Ok(())
    }

    fn handle_add_rdfs_label_property(&mut self, class_id: &IdOpt) -> Result<(), ProcessorError> {
        let rdfs_label = "rdfs:label".to_string();
        let rdfs_label_id = IdOpt::String(rdfs_label.clone());
        tracing::debug!(
            "Existing classes in vocabulary: {:#?}",
            self.class_properties.keys()
        );
        tracing::debug!("Adding rdfs:label property to class: {:#?}", class_id);
        let entry = self
            .class_properties
            .entry(class_id.clone())
            .and_modify(|entry| {
                if !entry.contains(&rdfs_label) {
                    entry.push(rdfs_label.clone());
                }
            });
        if matches!(entry, Entry::Vacant(_)) {
            let msg = format!(
                "Picklist Class {} not found in vocabulary while adding label",
                class_id
            );
            if self.is_strict {
                return Err(ProcessorError::Processing(msg));
            } else {
                self.processing_state.add_warning(
                    format!("{}, skipping label addition", msg),
                    Some("vocabulary_processing".to_string()),
                );
                return Ok(());
            }
        }

        let rdfs_label_property_entry = self.vocabulary.properties.entry(rdfs_label_id.clone());
        match rdfs_label_property_entry {
            std::collections::hash_map::Entry::Vacant(_) => {
                let property_term = VocabularyTerm {
                    id: rdfs_label_id.clone(),
                    type_: vec!["rdf:Property".to_string()],
                    sub_class_of: None,
                    label: Some("label".to_string()),
                    comment: Some("The human-readable label of the resource".to_string()),
                    domain: Some(vec![class_id.final_iri()]),
                    range: Some(vec![PropertyDatatype::String]),
                    extra_items: HashMap::new(),
                    one_of: None,
                };
                self.vocabulary
                    .properties
                    .insert(rdfs_label_id, property_term);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().update_with(VocabularyTerm {
                    id: rdfs_label_id,
                    type_: vec!["rdf:Property".to_string()],
                    sub_class_of: None,
                    label: Some("label".to_string()),
                    comment: Some("The human-readable label of the resource".to_string()),
                    domain: Some(vec![class_id.final_iri()]),
                    range: Some(vec![PropertyDatatype::String]),
                    extra_items: HashMap::new(),
                    one_of: None,
                })?;
            }
        }

        Ok(())
    }

    pub fn take_vocabulary(&mut self) -> (VocabularyMap, ProcessingState) {
        let vocabulary = std::mem::take(&mut self.vocabulary);
        let state = std::mem::take(&mut self.processing_state);

        (vocabulary, state)
    }
}
