use std::collections::HashMap;
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;

use super::mapping::{MappingConfig, RowValues, VocabularyColumnMapping};
use crate::error::ProcessorError;
use crate::manifest::{ImportStep, ModelStep, StepType};
use crate::types::{ExtraItem, IdOpt, OnEntity, VocabularyMap, VocabularyTerm};
use crate::utils::{map_xsd_type, to_camel_case, to_pascal_case};
use crate::{contains_variant, Manifest};

pub struct VocabularyProcessor {
    manifest: Arc<Manifest>,
    vocabulary: VocabularyMap,
    class_properties: HashMap<IdOpt, Vec<String>>,
    is_strict: bool,
}

impl VocabularyProcessor {
    pub fn new(manifest: Arc<Manifest>, is_strict: bool) -> Self {
        Self {
            manifest,
            vocabulary: VocabularyMap::new(),
            class_properties: HashMap::new(),
            is_strict,
        }
    }

    pub fn get_vocabulary(&self) -> VocabularyMap {
        VocabularyMap {
            classes: self.vocabulary.classes.clone(),
            properties: self.vocabulary.properties.clone(),
            identifiers: self.vocabulary.identifiers.clone(),
        }
    }

    pub fn from_headers(
        headers: &csv::StringRecord,
        mut step: ImportStep,
        is_strict: bool,
    ) -> Result<MappingConfig, ProcessorError> {
        // Default column names
        let mut mapping = if contains_variant!(
            &step.types,
            StepType::ModelStep(ModelStep::BasicVocabularyStep)
        ) {
            MappingConfig {
                type_: StepType::ModelStep(ModelStep::BasicVocabularyStep),
                column_mapping: VocabularyColumnMapping::basic_vocabulary_step(),
            }
        } else if contains_variant!(
            &step.types,
            StepType::ModelStep(ModelStep::SubClassVocabularyStep)
        ) {
            MappingConfig {
                type_: StepType::ModelStep(ModelStep::SubClassVocabularyStep),
                column_mapping: VocabularyColumnMapping::sub_class_vocabulary_step(),
            }
        } else {
            tracing::error!("Step Error: {:#?}", step);
            return Err(ProcessorError::Processing("Invalid step type".into()));
        };

        // Apply any overrides from the manifest
        for override_ in &step.overrides {
            let mapping_type = &mapping.type_;
            mapping
                .column_mapping
                .handle_override(override_, mapping_type)?;
        }

        if let Some(replace_id_with) = &step.replace_id_with {
            mapping.column_mapping.replace_id_with(replace_id_with)?;
        }

        for extra_item in step.extra_items.drain(..) {
            mapping
                .column_mapping
                .extra_items
                .insert(extra_item.column.clone(), extra_item);
        }

        let required_class_id_columns = match &mapping.column_mapping.class_column {
            IdOpt::String(class_column) => vec![(class_column, "Class ID")],
            IdOpt::ReplacementMap {
                original_id,
                replacement_id,
            } => vec![
                (original_id, original_id.as_str()),
                (replacement_id, replacement_id.as_str()),
            ],
        };

        // Verify all required columns exist
        let required_columns = match &mapping.type_ {
            StepType::ModelStep(ModelStep::BasicVocabularyStep) => vec![
                (
                    mapping.column_mapping.property_column.as_ref().unwrap(),
                    "Property Name",
                ),
                (mapping.column_mapping.type_column.as_ref().unwrap(), "Type"),
            ],
            StepType::ModelStep(ModelStep::SubClassVocabularyStep) => {
                vec![(&mapping.column_mapping.class_label_column, "Class Name")]
            }
            _ => return Err(ProcessorError::Processing("Invalid step type".into())),
        };

        let required_columns = required_columns
            .into_iter()
            .chain(required_class_id_columns)
            .collect::<Vec<_>>();

        tracing::debug!("Required columns: {:?}", required_columns);
        tracing::debug!("Mapping config: {:#?}", mapping);

        for (column, name) in required_columns.iter() {
            if !headers.iter().any(|h| h == column.as_str()) {
                return Err(ProcessorError::Processing(format!(
                    "Required column '{}' not found in CSV headers",
                    name
                )));
            }
        }

        let mut json_value_of_mapping = serde_json::to_value(&mapping.column_mapping).unwrap();
        let json_object_of_mapping = json_value_of_mapping.as_object_mut().unwrap();
        for (extra_item_column, extra_item) in mapping.column_mapping.extra_items.iter() {
            json_object_of_mapping.insert(
                extra_item.map_to.to_string(),
                serde_json::Value::String(extra_item_column.clone()),
            );
        }
        for (concept_to_extract, expected_csv_header) in json_object_of_mapping.iter() {
            if concept_to_extract == "extra_items" {
                continue;
            }
            if !headers
                .iter()
                .any(|h| h == expected_csv_header.as_str().unwrap())
            {
                if is_strict {
                    return Err(ProcessorError::Processing(format!(
                        "Column '{}' not found in CSV headers. If this is acceptable, run again without --strict",
                        expected_csv_header
                    )));
                } else {
                    tracing::warn!(
                        "Column '{}' not found in CSV headers. If this is not expected, check your CSV file for typos or missing columns, or update the manifest to match the CSV file",
                        expected_csv_header
                    );
                }
            }
        }

        Ok(mapping)
    }

    pub async fn process_vocabulary(
        &mut self,
        step: ImportStep,
        model_path: &str,
    ) -> Result<(), ProcessorError> {
        let file_path = PathBuf::from(model_path).join(&step.path);
        tracing::debug!("Reading vocabulary data from {:?}", file_path);

        let mut rdr: csv::Reader<std::fs::File> =
            csv::Reader::from_path(&file_path).map_err(|e| {
                ProcessorError::Processing(format!(
                    "Failed to read CSV @ {}: {}",
                    &file_path.to_string_lossy(),
                    e
                ))
            })?;

        // Get headers and build column mapping
        let headers = rdr
            .headers()
            .map_err(|e| ProcessorError::Processing(format!("Failed to read CSV headers: {}", e)))?
            .clone();

        tracing::debug!("CSV headers: {:?}", headers);

        let sub_class_of = step.sub_class_of.clone();

        let mapping = Self::from_headers(&headers, step, self.is_strict)?;

        tracing::debug!("Vocabulary column mapping: {:?}", mapping);

        // Process each row
        for result in rdr.records() {
            let record = result.map_err(|e| {
                ProcessorError::Processing(format!("Failed to read CSV record: {}", e))
            })?;

            let row_values = mapping.extract_values(&record, &headers)?;

            self.process_class_term(&row_values, sub_class_of.clone())?;
            if !matches!(
                &mapping.type_,
                StepType::ModelStep(ModelStep::SubClassVocabularyStep)
            ) {
                self.process_property_term(&row_values)?;
            }
        }

        // Update class terms with their properties
        for (class_id, properties) in &self.class_properties {
            if let Some(class_term) = self.vocabulary.classes.get_mut(class_id) {
                class_term.range = Some(
                    properties
                        .iter()
                        .map(|p| format!("{}{}", self.manifest.model.base_iri, p))
                        .collect(),
                );
            }
        }

        Ok(())
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
        for (_, extra_item) in extra_items {
            if matches!(extra_item.on_entity, OnEntity::Class) {
                extra_items_result.insert(
                    extra_item.map_to.clone(),
                    extra_item.value.clone().unwrap_or("".to_string()),
                );
            }
        }

        let label = if class_name.is_empty() {
            tracing::debug!("Class name is empty, using class_id");
            match class_id {
                IdOpt::String(class_id) => class_id.to_string(),
                IdOpt::ReplacementMap { replacement_id, .. } => replacement_id.to_string(),
            }
        } else {
            class_name.to_string()
        };

        if let std::collections::hash_map::Entry::Vacant(_) =
            self.vocabulary.classes.entry(class_id.clone())
        {
            let class_term = VocabularyTerm {
                id: class_id
                    .to_pascal_case()
                    .with_base_iri(&self.manifest.model.base_iri),
                type_: vec!["rdfs:Class".to_string()],
                sub_class_of,
                label,
                comment: Some(class_description.to_string()),
                domain: None,
                range: Some(vec![]),
                extra_items: extra_items_result,
            };

            self.vocabulary.classes.insert(class_id.clone(), class_term);
        }
        Ok(())
    }

    fn process_property_term(&mut self, row_values: &RowValues) -> Result<(), ProcessorError> {
        /*
        property: &str,
        property_desc: &str,
        property_type: &str,
        property_class: &str,
        class_name: &str,
        extra_items: HashMap<String, String>,
         */
        let RowValues {
            property_id,
            property_description,
            property_type,
            property_class,
            class_id,
            class_name,
            extra_items,
            ..
        } = row_values;

        tracing::debug!("[RowValues] class_id: {:#?}", class_id);

        let property = property_id.unwrap();
        let property_desc = property_description.unwrap();
        let property_type = property_type.unwrap();
        let property_class = property_class.unwrap();
        let extra_items = extra_items.clone();

        if !property.is_empty() {
            let xsd_type = map_xsd_type(property_type);
            let camel_name = to_camel_case(property);
            let range = if !property_class.is_empty() {
                let value = format!(
                    "{}{}",
                    self.manifest.model.base_iri,
                    to_pascal_case(property_class)
                );
                Some(vec![value])
            } else {
                Some(vec![format!("xsd:{}", xsd_type)])
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

            // Create property term
            let property_term = VocabularyTerm {
                id: IdOpt::String(format!("{}{}", self.manifest.model.base_iri, camel_name)),
                type_: vec!["rdf:Property".to_string()],
                sub_class_of: None,
                label: property.to_string(),
                comment: Some(property_desc.to_string()),
                domain: Some(vec![class_id
                    .to_pascal_case()
                    .with_base_iri(&self.manifest.model.base_iri)
                    .final_iri()]),
                range,
                extra_items: extra_items_result,
            };

            // If it's an ID property, store it in identifiers map
            if xsd_type == "ID" {
                self.vocabulary
                    .identifiers
                    .insert(class_id.to_pascal_case().to_string(), property_term);
            } else {
                // Otherwise store it in properties map
                match self
                    .vocabulary
                    .properties
                    .entry(IdOpt::String(camel_name.clone()))
                {
                    std::collections::hash_map::Entry::Vacant(_) => {
                        self.vocabulary
                            .properties
                            .insert(IdOpt::String(camel_name), property_term);
                    }
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().update_with(property_term)?
                    }
                }

                // Track properties for each class
                if !class_name.is_empty() {
                    let class_entry = self.class_properties.entry(class_id.clone()).or_default();

                    if !property.is_empty() {
                        class_entry.push(to_camel_case(property));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn take_vocabulary(&mut self) -> VocabularyMap {
        std::mem::take(&mut self.vocabulary)
    }
}
