use super::types::InstanceProcessor;
use crate::error::ProcessorError;
use crate::manifest::{ImportStep, InstanceStep, StepType};
use crate::types::{IdOpt, JsonLdInstance};
use crate::utils::{to_kebab_case, to_pascal_case};
use serde_json::Map;
use std::collections::hash_map::Entry;
use std::path::PathBuf;
use uuid::Uuid;

impl InstanceProcessor {
    pub async fn process_simple_instance(
        &mut self,
        step: &ImportStep,
        instance_path: &str,
    ) -> Result<(), ProcessorError> {
        let is_picklist_step = step
            .types
            .iter()
            .any(|t| matches!(t, StepType::InstanceStep(InstanceStep::PicklistStep)));

        let mut class_type = step.instance_type.clone();

        if class_type.is_empty() {
            let file_path = PathBuf::from(&step.path);
            let file_name = file_path.file_stem().unwrap().to_str().unwrap();
            class_type = to_pascal_case(file_name);
            self.processing_state.add_warning(
                format!("No explicit instance type provided for CSV at path: {}. Using CSV name as default: {}", &step.path, &class_type),
                Some("instance_processing".to_string()),
            );
        }

        let vocab = self.vocabulary.as_ref().ok_or_else(|| {
            ProcessorError::Processing("Vocabulary must be set before processing instances".into())
        })?;

        let override_label = step
            .overrides
            .iter()
            .find(|over_ride| over_ride.map_to == "@id")
            .map(|over_ride| &over_ride.column);

        let identifier_label = vocab.get_identifier_label(&class_type).or(override_label).ok_or_else(|| {
            ProcessorError::Processing(format!(
                "No identifier property found for class '{}'. To import instances of this class, your data model CSVs must indicate which column of the instance data will be used as the identifier (\"@id\") for instances of this class. Or, you must provide an \"override\" where the appropriate column maps to \"@id\".",
                class_type
            ))
        })?.clone();

        let file_path = PathBuf::from(instance_path).join(&step.path);

        let mut rdr = csv::Reader::from_path(&file_path).map_err(|e| {
            ProcessorError::Processing(format!(
                "Failed to read CSV @ {}: {}",
                &file_path.to_string_lossy(),
                e
            ))
        })?;

        let headers: Vec<String> = rdr
            .headers()
            .map_err(|e| ProcessorError::Processing(format!("Failed to read CSV headers: {}", e)))?
            .iter()
            .map(|h| h.to_string())
            .collect();

        if let Some(pivot_columns) = &step.pivot_columns {
            self.validate_pivot_columns(pivot_columns.iter().collect(), &class_type)?;
        };

        let headers = self.validate_headers(
            &headers,
            &class_type,
            &identifier_label,
            step.pivot_columns.as_ref(),
            step.map_to_label.as_ref(),
        )?;

        let id_column_index = headers
            .iter()
            .position(|h| {
                h.as_ref()
                    .map(|h| h.name == *identifier_label)
                    .unwrap_or(false)
            })
            .ok_or_else(|| {
                ProcessorError::Processing(format!(
                    "Identifier column '{}' not found in headers: {}",
                    identifier_label,
                    headers
                        .iter()
                        .filter_map(|opt_head| opt_head.as_ref().map(|h| h.name.clone()))
                        .collect::<Vec<String>>()
                        .join(", ")
                ))
            })?;

        for (result_row_num, result) in rdr.records().enumerate() {
            let record = result.map_err(|e| {
                ProcessorError::Processing(format!("Failed to read CSV record: {}", e))
            })?;

            let mut id = record
                .get(id_column_index)
                .ok_or_else(|| ProcessorError::Processing("Missing identifier value".into()))?
                .to_string();

            if self.is_namespace_iris {
                id = format!("{}/{}", to_kebab_case(&class_type), &id);
            }

            let mut properties = Map::new();

            if headers.len() != record.len() {
                return Err(ProcessorError::Processing(format!(
                    "Row has different number of columns than headers: RECORD: {} HEADERS: {}",
                    record.len(),
                    headers.len()
                )));
            }

            for (i, header) in headers.iter().enumerate() {
                if let Some(header) = header {
                    if let Some(value) = record.get(i) {
                        if !value.is_empty() {
                            let is_pivot_header =
                                step.pivot_columns.as_ref().and_then(|pivot_columns| {
                                    pivot_columns.iter().find(|pivot_column| {
                                        pivot_column.columns.contains(&header.name)
                                    })
                                });

                            let vec_value = if let Some(delimiter) = step.delimit_values_on.as_ref()
                            {
                                if header.datatype == crate::types::PropertyDatatype::String {
                                    vec![value]
                                } else {
                                    value.split(delimiter.as_str()).map(|s| s.trim()).collect()
                                }
                            } else {
                                vec![value]
                            };

                            let mut final_values = vec![];

                            for value in vec_value {
                                final_values.push(self.process_value(
                                    value,
                                    &header.datatype,
                                    &header.name,
                                    result_row_num,
                                )?);
                            }

                            if let Some(pivot_column_match) = is_pivot_header {
                                let pivot_property_entry = properties
                                    .entry(pivot_column_match.new_relationship_property.clone())
                                    .or_insert_with(|| {
                                        let id = Uuid::new_v4().to_string();
                                        let mut new_map = Map::new();
                                        new_map.insert(
                                            "@id".to_string(),
                                            serde_json::Value::String(id),
                                        );
                                        serde_json::Value::Object(new_map)
                                    });
                                let id = pivot_property_entry.get("@id").unwrap().as_str().unwrap();
                                let mut properties = Map::new();
                                properties.insert(header.name.clone(), final_values.into());

                                let new_instance = JsonLdInstance {
                                    id: IdOpt::String(id.to_string()),
                                    type_: vec![IdOpt::String(
                                        pivot_column_match.instance_type.clone(),
                                    )],
                                    properties,
                                };

                                match self.instances.entry(id.to_string()) {
                                    Entry::Occupied(mut entry) => {
                                        entry.get_mut().update_with(new_instance)?;
                                    }
                                    Entry::Vacant(entry) => {
                                        entry.insert(new_instance);
                                    }
                                };
                            } else {
                                if header.is_label_header {
                                    properties
                                        .insert(header.name.clone(), final_values.clone().into());
                                    properties.insert("label".to_string(), final_values.into());
                                } else {
                                    properties.insert(header.name.clone(), final_values.into());
                                }
                            }
                        }
                    }
                }
            }

            let instance_id = IdOpt::String(id);

            let instance = JsonLdInstance {
                id: instance_id.clone(),
                type_: vec![IdOpt::String(class_type.clone())],
                properties,
            };

            self.update_or_insert_instance(instance.clone())?;
            if is_picklist_step {
                let final_instance_id =
                    instance_id.with_base_iri(&self.manifest.instances.base_iri);
                self.vocabulary
                    .as_mut()
                    .unwrap()
                    .update_or_insert_picklist_instance(class_type.clone(), final_instance_id)?;
            }
        }

        Ok(())
    }

    pub async fn process_subclass_instance(
        &mut self,
        step: &ImportStep,
        instance_path: &str,
    ) -> Result<(), ProcessorError> {
        // For now, just add a warning that this feature is not yet implemented
        self.processing_state.add_warning(
            "Subclass instance processing is not yet implemented".to_string(),
            Some("instance_processing".to_string()),
        );
        Ok(())
    }

    pub async fn process_properties_instance(
        &mut self,
        step: &ImportStep,
        instance_path: &str,
    ) -> Result<(), ProcessorError> {
        // For now, just add a warning that this feature is not yet implemented
        self.processing_state.add_warning(
            "Properties instance processing is not yet implemented".to_string(),
            Some("instance_processing".to_string()),
        );
        Ok(())
    }

    pub(crate) fn update_or_insert_instance(
        &mut self,
        instance: JsonLdInstance,
    ) -> Result<(), ProcessorError> {
        let id = instance.id.clone();

        match self.instances.entry(id.to_string()) {
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
