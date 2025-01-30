use super::types::InstanceProcessor;
use crate::error::ProcessorError;
use crate::manifest::{ImportStep, InstanceStep, StepType};
use crate::types::{IdOpt, JsonLdInstance, PropertyDatatype};
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
            let record = match result {
                Ok(record) => record,
                Err(e) => {
                    let msg = format!("Failed to read CSV record: {}", e);
                    if self.is_strict {
                        return Err(ProcessorError::Processing(msg));
                    } else {
                        self.processing_state.add_warning(
                            format!("{}, skipping row", msg),
                            Some("csv_processing".to_string()),
                        );
                        continue;
                    }
                }
            };

            let id = match record.get(id_column_index) {
                Some(id) if !id.is_empty() => id.to_string(),
                _ => {
                    let msg = format!(
                        "Missing or empty identifier value at row {}",
                        result_row_num + 1
                    );
                    if self.is_strict {
                        return Err(ProcessorError::Processing(msg));
                    } else {
                        self.processing_state.add_warning(
                            format!("{}, skipping row", msg),
                            Some("instance_processing".to_string()),
                        );
                        continue;
                    }
                }
            };

            let id = if self.is_namespace_iris {
                format!("{}/{}", to_kebab_case(&class_type), &id)
            } else {
                id
            };

            let mut properties = Map::new();

            if headers.len() != record.len() {
                let msg = format!(
                    "Row {} has different number of columns than headers: RECORD: {} HEADERS: {}",
                    result_row_num + 1,
                    record.len(),
                    headers.len()
                );
                if self.is_strict {
                    return Err(ProcessorError::Processing(msg));
                } else {
                    self.processing_state.add_warning(
                        format!("{}, skipping row", msg),
                        Some("csv_processing".to_string()),
                    );
                    continue;
                }
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

                            if matches!(&header.datatype, PropertyDatatype::ID) {
                                continue;
                            }

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
                                        if let Err(e) = entry.get_mut().update_with(new_instance) {
                                            let msg = format!(
                                                "Failed to update pivot instance {}: {}",
                                                id, e
                                            );
                                            if self.is_strict {
                                                return Err(ProcessorError::Processing(msg));
                                            } else {
                                                self.processing_state.add_warning(
                                                    format!("{}, using original values", msg),
                                                    Some("instance_processing".to_string()),
                                                );
                                            }
                                        }
                                    }
                                    Entry::Vacant(entry) => {
                                        entry.insert(new_instance);
                                    }
                                };
                            } else if header.is_label_header {
                                properties.insert(header.name.clone(), final_values.clone().into());
                                properties.insert("label".to_string(), final_values.into());
                            } else {
                                properties.insert(header.name.clone(), final_values.into());
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

            if let Err(e) = self.update_or_insert_instance(instance.clone()) {
                let msg = format!("Failed to update/insert instance {}: {}", instance.id, e);
                if self.is_strict {
                    return Err(ProcessorError::Processing(msg));
                } else {
                    self.processing_state.add_warning(
                        format!("{}, skipping instance", msg),
                        Some("instance_processing".to_string()),
                    );
                    continue;
                }
            }
            if is_picklist_step {
                let final_instance_id = instance_id.clone();
                let final_instance_id_with_base =
                    final_instance_id.with_base_iri(&self.manifest.instances.base_iri);
                if let Err(e) = self
                    .vocabulary
                    .as_mut()
                    .unwrap()
                    .update_or_insert_picklist_instance(
                        class_type.clone(),
                        final_instance_id_with_base,
                    )
                {
                    let msg = format!(
                        "Failed to update picklist instance {}: {}",
                        final_instance_id, e
                    );
                    if self.is_strict {
                        return Err(ProcessorError::Processing(msg));
                    } else {
                        self.processing_state
                            .add_warning(msg, Some("picklist_processing".to_string()));
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn process_subclass_instance(
        &mut self,
        step: &ImportStep,
        instance_path: &str,
    ) -> Result<(), ProcessorError> {
        // Get the parent class type from the manifest
        let parent_class_type = step.instance_type.clone();

        // Get the subclass property that indicates which subclass each instance belongs to
        let subclass_property = step.sub_class_property.as_ref().ok_or_else(|| {
            ProcessorError::Processing(
                "SubClassInstanceStep requires subClassProperty field".into(),
            )
        })?;

        let vocab = self.vocabulary.as_ref().ok_or_else(|| {
            ProcessorError::Processing("Vocabulary must be set before processing instances".into())
        })?;

        tracing::debug!("Getting identifier label for class '{}'", parent_class_type);

        // Check for an override column for identifier label
        let override_label = step
            .overrides
            .iter()
            .find(|over_ride| over_ride.map_to == "@id")
            .map(|over_ride| &over_ride.column);

        // Get the identifier property for this class
        let identifier_label = vocab
            .get_identifier_label(&parent_class_type)
            .or(override_label)
            .ok_or_else(|| {
                ProcessorError::Processing(format!(
                    "No identifier property found for class '{}'",
                    parent_class_type
                ))
            })?
            .clone();

        let file_path = PathBuf::from(instance_path).join(&step.path);
        tracing::debug!("Reading instance data from {:?}", file_path);

        let mut rdr = csv::Reader::from_path(&file_path).map_err(|e| {
            ProcessorError::Processing(format!(
                "Failed to read CSV @ {}: {}",
                &file_path.to_string_lossy(),
                e
            ))
        })?;

        // Read headers and find important column indices
        let headers = rdr
            .headers()
            .map_err(|e| ProcessorError::Processing(format!("Failed to read CSV headers: {}", e)))?
            .clone();

        let id_column_index = headers
            .iter()
            .position(|h| h == identifier_label)
            .ok_or_else(|| {
                ProcessorError::Processing(format!(
                    "Identifier column '{}' not found in headers of CSV ({}): {:#?}",
                    identifier_label, step.path, headers
                ))
            })?;

        let subclass_column_index = headers
            .iter()
            .position(|h| h == subclass_property)
            .ok_or_else(|| {
                ProcessorError::Processing(format!(
                    "Subclass property column '{}' not found in headers",
                    subclass_property
                ))
            })?;

        // Process each row
        for (result_row_num, result) in rdr.records().enumerate() {
            let record = match result {
                Ok(record) => record,
                Err(e) => {
                    let msg = format!("Failed to read CSV record: {}", e);
                    if self.is_strict {
                        return Err(ProcessorError::Processing(msg));
                    } else {
                        self.processing_state.add_warning(
                            format!("{}, skipping row", msg),
                            Some("csv_processing".to_string()),
                        );
                        continue;
                    }
                }
            };

            // Get the identifier value
            let id = match record.get(id_column_index) {
                Some(id) if !id.is_empty() => id.to_string(),
                _ => {
                    let msg = format!(
                        "Missing or empty identifier value at row {}",
                        result_row_num + 1
                    );
                    if self.is_strict {
                        return Err(ProcessorError::Processing(msg));
                    } else {
                        self.processing_state.add_warning(
                            format!("{}, skipping row", msg),
                            Some("instance_processing".to_string()),
                        );
                        continue;
                    }
                }
            };

            let id = if self.is_namespace_iris {
                format!("{}/{}", to_kebab_case(&parent_class_type), &id)
            } else {
                id
            };

            // Get the subclass reference
            let subclass_ref = match record.get(subclass_column_index) {
                Some(value) if !value.is_empty() => value,
                _ => {
                    let msg = format!(
                        "Missing or empty subclass reference at row {}",
                        result_row_num + 1
                    );
                    if self.is_strict {
                        return Err(ProcessorError::Processing(msg));
                    } else {
                        self.processing_state.add_warning(
                            format!("{}, skipping row", msg),
                            Some("instance_processing".to_string()),
                        );
                        continue;
                    }
                }
            };

            let subclass_ref = self
                .vocabulary
                .as_ref()
                .unwrap()
                .classes
                .iter()
                .find_map(|(id, _)| match id {
                    IdOpt::String(string_id) if string_id == subclass_ref => Some(id.clone()),
                    IdOpt::ReplacementMap { original_id, .. } if original_id == subclass_ref => {
                        Some(id.clone())
                    }
                    _ => None,
                })
                .unwrap_or_else(|| {
                    let msg = format!(
                        "Subclass reference '{}' not found in vocabulary",
                        subclass_ref
                    );
                    if self.is_strict {
                        panic!("{}", msg); // This will be caught and converted to an error
                    } else {
                        self.processing_state.add_warning(
                            format!("{}, using raw value", msg),
                            Some("instance_processing".to_string()),
                        );
                        IdOpt::String(subclass_ref.to_string())
                    }
                })
                .normalize()
                .to_pascal_case();

            let mut properties = Map::new();

            if headers.len() != record.len() {
                let msg = format!(
                    "Row {} has different number of columns than headers: RECORD: {} HEADERS: {}",
                    result_row_num + 1,
                    record.len(),
                    headers.len()
                );
                if self.is_strict {
                    return Err(ProcessorError::Processing(msg));
                } else {
                    self.processing_state.add_warning(
                        format!("{}, skipping row", msg),
                        Some("csv_processing".to_string()),
                    );
                    continue;
                }
            }

            // Map CSV columns to JSON-LD properties (excluding id and subclass columns)
            for (i, header) in headers.iter().enumerate() {
                if i != id_column_index && i != subclass_column_index {
                    if let Some(value) = record.get(i) {
                        if !value.is_empty() {
                            // TODO: Process value based on property type from vocabulary
                            // For now, just store as string
                            properties.insert(
                                header.to_string(),
                                serde_json::Value::String(value.to_string()),
                            );
                        }
                    }
                }
            }

            // Create instance with both parent class and subclass types
            let instance = JsonLdInstance {
                id: IdOpt::String(id),
                type_: vec![IdOpt::String(parent_class_type.clone()), subclass_ref],
                properties,
            };

            if let Err(e) = self.update_or_insert_instance(instance.clone()) {
                let msg = format!("Failed to update/insert instance {}: {}", instance.id, e);
                if self.is_strict {
                    return Err(ProcessorError::Processing(msg));
                } else {
                    self.processing_state.add_warning(
                        format!("{}, skipping instance", msg),
                        Some("instance_processing".to_string()),
                    );
                    continue;
                }
            }
        }

        Ok(())
    }

    pub async fn process_properties_instance(
        &mut self,
        step: &ImportStep,
        instance_path: &str,
    ) -> Result<(), ProcessorError> {
        let class_type = step.instance_type.clone();

        let vocab = self.vocabulary.as_ref().ok_or_else(|| {
            ProcessorError::Processing("Vocabulary must be set before processing instances".into())
        })?;

        // Check for an override column for identifier label
        let override_label = step
            .overrides
            .iter()
            .find(|over_ride| over_ride.map_to == "@id")
            .map(|over_ride| &over_ride.column);

        // Get the identifier property for this class
        let identifier_label = vocab
            .get_identifier_label(&class_type)
            .or(override_label)
            .ok_or_else(|| {
                ProcessorError::Processing(format!(
                    "No identifier property found for class '{}'",
                    class_type
                ))
            })?
            .clone();

        // Get property ID and value column names from overrides or use defaults
        let property_id_column = step
            .overrides
            .iter()
            .find(|o| o.map_to == "$Property.ID")
            .map(|o| o.column.as_str())
            .unwrap_or("Property ID");

        let property_value_column = step
            .overrides
            .iter()
            .find(|o| o.map_to == "$Property.Value")
            .map(|o| o.column.as_str())
            .unwrap_or("Property Value");

        let file_path = PathBuf::from(instance_path).join(&step.path);
        tracing::debug!("Reading instance data from {:?}", file_path);

        let mut rdr = csv::Reader::from_path(&file_path).map_err(|e| {
            ProcessorError::Processing(format!(
                "Failed to read CSV @ {}: {}",
                &file_path.to_string_lossy(),
                e
            ))
        })?;

        // Read headers and find required columns
        let headers = rdr.headers().map_err(|e| {
            ProcessorError::Processing(format!("Failed to read CSV headers: {}", e))
        })?;

        // Find column indices
        let id_column_index = headers
            .iter()
            .position(|h| h == identifier_label)
            .ok_or_else(|| {
                ProcessorError::Processing(format!(
                    "Identifier column '{}' not found in headers of CSV ({}): {:#?}",
                    identifier_label, step.path, headers
                ))
            })?;

        let property_id_index = headers
            .iter()
            .position(|h| h == property_id_column)
            .ok_or_else(|| {
                ProcessorError::Processing(format!(
                    "Property ID column '{}' not found in headers",
                    property_id_column
                ))
            })?;

        let property_value_index = headers
            .iter()
            .position(|h| h == property_value_column)
            .ok_or_else(|| {
                ProcessorError::Processing(format!(
                    "Property Value column '{}' not found in headers",
                    property_value_column
                ))
            })?;

        // Process each row
        for (result_row_num, result) in rdr.records().enumerate() {
            let record = match result {
                Ok(record) => record,
                Err(e) => {
                    let msg = format!("Failed to read CSV record: {}", e);
                    if self.is_strict {
                        return Err(ProcessorError::Processing(msg));
                    } else {
                        self.processing_state.add_warning(
                            format!("{}, skipping row", msg),
                            Some("csv_processing".to_string()),
                        );
                        continue;
                    }
                }
            };

            let entity_id = match record.get(id_column_index) {
                Some(id) if !id.is_empty() => id.to_string(),
                _ => {
                    let msg = format!(
                        "Missing or empty identifier value at row {}",
                        result_row_num + 1
                    );
                    if self.is_strict {
                        return Err(ProcessorError::Processing(msg));
                    } else {
                        self.processing_state.add_warning(
                            format!("{}, skipping row", msg),
                            Some("instance_processing".to_string()),
                        );
                        continue;
                    }
                }
            };

            let entity_id = if self.is_namespace_iris {
                format!("{}/{}", to_kebab_case(&class_type), entity_id)
            } else {
                entity_id
            };

            let property_id = match record.get(property_id_index) {
                Some(id) if !id.is_empty() => id,
                _ => {
                    let msg = format!("Missing or empty Property ID at row {}", result_row_num + 1);
                    if self.is_strict {
                        return Err(ProcessorError::Processing(msg));
                    } else {
                        self.processing_state.add_warning(
                            format!("{}, skipping row", msg),
                            Some("instance_processing".to_string()),
                        );
                        continue;
                    }
                }
            };

            let property_id = vocab
                .properties
                .keys()
                .find_map(|id| match id {
                    IdOpt::String(string_id) if string_id == property_id => Some(id.clone()),
                    IdOpt::ReplacementMap { original_id, .. } if original_id == property_id => {
                        Some(id.clone())
                    }
                    _ => None,
                })
                .unwrap_or_else(|| {
                    let msg = format!("Property ID '{}' not found in vocabulary", property_id);
                    if self.is_strict {
                        panic!("{}", msg); // This will be caught and converted to an error
                    } else {
                        self.processing_state.add_warning(
                            format!("{}, using raw value", msg),
                            Some("instance_processing".to_string()),
                        );
                        IdOpt::String(property_id.to_string())
                    }
                });

            let property_value = match record.get(property_value_index) {
                Some(value) if !value.is_empty() => value,
                _ => {
                    let msg = format!(
                        "Missing or empty Property Value at row {}",
                        result_row_num + 1
                    );
                    if self.is_strict {
                        return Err(ProcessorError::Processing(msg));
                    } else {
                        self.processing_state.add_warning(
                            format!("{}, skipping row", msg),
                            Some("instance_processing".to_string()),
                        );
                        continue;
                    }
                }
            };

            // Create or update instance
            self.instances
                .entry(entity_id.to_string())
                .and_modify(|instance| {
                    // Instance already exists, modify it as needed
                    instance
                        .properties
                        .entry(property_id.final_iri())
                        .and_modify(|current| {
                            if current.is_array() {
                                // Add to existing array
                                let array = current.as_array_mut().unwrap();
                                array.push(serde_json::Value::String(property_value.to_string()));
                            } else {
                                // Convert to array with both values
                                let array = vec![
                                    current.take(),
                                    serde_json::Value::String(property_value.to_string()),
                                ];
                                *current = serde_json::Value::Array(array);
                            }
                        })
                        .or_insert_with(|| serde_json::Value::String(property_value.to_string()));
                })
                .or_insert_with(|| {
                    // Instance does not exist, create it
                    let mut properties = Map::new();
                    properties.insert(
                        property_id.final_iri(),
                        serde_json::Value::String(property_value.to_string()),
                    );
                    JsonLdInstance {
                        id: IdOpt::String(entity_id),
                        type_: vec![IdOpt::String(class_type.clone())],
                        properties,
                    }
                });
        }

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
