use csv::StringRecord;
use serde::Serialize;
use std::collections::HashMap;

use crate::error::{ProcessingState, ProcessorError};
use crate::manifest::{ModelStep, StepType};
use crate::types::{ColumnOverride, ExtraItem, IdOpt};
use crate::utils::validate_column_identifier;

#[derive(Debug)]
pub struct RowValues<'a> {
    pub class_id: IdOpt,
    pub class_name: Option<&'a str>,
    pub class_description: Option<&'a str>,
    pub property_id: Option<IdOpt>,
    pub property_name: Option<&'a str>,
    pub property_description: Option<&'a str>,
    pub property_type: Option<&'a str>,
    pub property_class: Option<&'a str>,
    pub extra_items: HashMap<String, ExtraItem>,
}

#[derive(Debug)]
pub struct MappingConfig {
    pub type_: StepType,
    pub column_mapping: VocabularyColumnMapping,
    pub is_strict: bool,
    pub processing_state: ProcessingState,
}

impl MappingConfig {
    pub fn new(type_: StepType, column_mapping: VocabularyColumnMapping, is_strict: bool) -> Self {
        Self {
            type_,
            column_mapping,
            is_strict,
            processing_state: ProcessingState::new(),
        }
    }

    pub fn extract_values<'a>(
        &mut self,
        record: &'a StringRecord,
        headers: &StringRecord,
    ) -> Result<RowValues<'a>, ProcessorError> {
        match self.type_ {
            StepType::ModelStep(ModelStep::BasicVocabularyStep) => {
                self.extract_basic_vocabulary_values(record, headers)
            }
            StepType::ModelStep(ModelStep::SubClassVocabularyStep) => {
                self.extract_sub_class_vocabulary_values(record, headers)
            }
            StepType::ModelStep(ModelStep::PropertiesVocabularyStep) => {
                self.extract_properties_vocabulary_values(record, headers)
            }
            _ => Err(ProcessorError::InvalidManifest(
                "Invalid StepType for MappingConfig".to_string(),
            )),
        }
    }

    fn extract_basic_vocabulary_values<'a>(
        &mut self,
        record: &'a StringRecord,
        headers: &StringRecord,
    ) -> Result<RowValues<'a>, ProcessorError> {
        let mapping = &self.column_mapping;
        let class_id = mapping
            .get_id_value(record, headers, &mapping.class_column)
            .ok_or_else(|| ProcessorError::Processing("Missing Class ID value".into()))?;
        let class_name = mapping
            .get_value(
                record,
                headers,
                mapping.class_label_column.as_deref().unwrap_or(""),
            )
            .unwrap_or("");
        let class_desc = mapping
            .get_value(
                record,
                headers,
                mapping.class_description_column.as_deref().unwrap_or(""),
            )
            .unwrap_or("");
        let extra_items = mapping
            .extra_items
            .values()
            .map(|extra_item| {
                let value = mapping
                    .get_value(record, headers, &extra_item.column)
                    .unwrap_or("")
                    .to_string();
                let extra_item = extra_item.set_value(value);
                (extra_item.map_to.clone(), extra_item)
            })
            .collect::<HashMap<_, _>>();

        let property = match mapping.property_column.as_ref() {
            Some(column) => mapping
                .get_id_value(record, headers, column)
                .ok_or_else(|| ProcessorError::Processing("Missing Property value".into()))
                .or_else(|err| {
                    if self.is_strict {
                        Err(err)
                    } else {
                        self.processing_state.add_warning(
                            "Missing Property value, using empty string",
                            Some("property_validation".to_string()),
                        );
                        Ok(IdOpt::String("".to_string()))
                    }
                })?,
            None => {
                return Ok(RowValues {
                    class_id,
                    class_name: Some(class_name),
                    class_description: Some(class_desc),
                    property_id: None,
                    property_name: None,
                    property_description: None,
                    property_type: None,
                    property_class: None,
                    extra_items,
                })
            }
        };
        let property_name = mapping
            .get_value(
                record,
                headers,
                mapping.property_name_column.as_ref().unwrap(),
            )
            .unwrap_or("");
        let property_desc = mapping
            .get_value(
                record,
                headers,
                mapping
                    .property_description_column
                    .as_ref()
                    .unwrap_or(&"".to_string()),
            )
            .unwrap_or("");
        let property_type = mapping
            .get_value(
                record,
                headers,
                mapping.type_column.as_ref().unwrap_or(&"".to_string()),
            )
            .unwrap_or("string"); // Default to string type
        let property_class = mapping
            .get_value(
                record,
                headers,
                mapping
                    .property_class_column
                    .as_ref()
                    .unwrap_or(&"".to_string()),
            )
            .unwrap_or("");
        let result = RowValues {
            class_id,
            class_name: Some(class_name),
            class_description: Some(class_desc),
            property_id: Some(property),
            property_name: Some(property_name),
            property_description: Some(property_desc),
            property_type: Some(property_type),
            property_class: Some(property_class),
            extra_items,
        };
        Ok(result)
    }

    fn extract_sub_class_vocabulary_values<'a>(
        &mut self,
        record: &'a StringRecord,
        headers: &StringRecord,
    ) -> Result<RowValues<'a>, ProcessorError> {
        let mapping = &self.column_mapping;
        let class_id = mapping
            .get_id_value(record, headers, &mapping.class_column)
            .ok_or_else(|| ProcessorError::Processing("Missing Class ID column".into()))?;
        let class_name = mapping
            .get_value(
                record,
                headers,
                mapping.class_label_column.as_deref().unwrap_or(""),
            )
            .unwrap_or("");
        let class_desc = mapping
            .get_value(
                record,
                headers,
                mapping.class_description_column.as_deref().unwrap_or(""),
            )
            .unwrap_or("");
        let extra_items = mapping
            .extra_items
            .values()
            .map(|extra_item| {
                let value = mapping
                    .get_value(record, headers, &extra_item.column)
                    .unwrap_or("")
                    .to_string();
                let extra_item = extra_item.set_value(value);
                (extra_item.map_to.clone(), extra_item)
            })
            .collect::<HashMap<_, _>>();

        let result = RowValues {
            class_id,
            class_name: Some(class_name),
            class_description: Some(class_desc),
            property_id: None,
            property_name: None,
            property_description: None,
            property_type: None,
            property_class: None,
            extra_items,
        };
        Ok(result)
    }

    fn extract_properties_vocabulary_values<'a>(
        &mut self,
        record: &'a StringRecord,
        headers: &StringRecord,
    ) -> Result<RowValues<'a>, ProcessorError> {
        let mapping = &self.column_mapping;
        let class_id = mapping
            .get_id_value(record, headers, &mapping.class_column)
            .ok_or_else(|| ProcessorError::Processing("Missing Class ID value".into()))?;
        let property = match mapping.property_column.as_ref() {
            Some(column) => mapping
                .get_id_value(record, headers, column)
                .ok_or_else(|| ProcessorError::Processing("Missing Property value".into()))
                .or_else(|e| {
                    if !self.is_strict {
                        self.processing_state.add_warning(
                            format!("{}, using empty string", e),
                            Some("property_validation".to_string()),
                        );
                        Ok(IdOpt::String("".to_string()))
                    } else {
                        Err(e)
                    }
                })?,
            None => IdOpt::String("".to_string()),
        };
        let property_name = mapping
            .get_value(
                record,
                headers,
                mapping.property_name_column.as_ref().unwrap(),
            )
            .unwrap_or("");
        if property_name == "MF1" {
            tracing::debug!(
                "[MF1] [extract_properties_vocabulary_values] record: {:?}",
                record
            );
            tracing::debug!(
                "[MF1] [extract_properties_vocabulary_values] headers: {:?}",
                headers
            );
            tracing::debug!(
                "[MF1] [extract_properties_vocabulary_values] property column mapping: {:?}",
                mapping.property_column.as_ref().unwrap()
            );
            tracing::debug!(
                "[MF1] [extract_properties_vocabulary_values] property: {:?}",
                property
            );
        }
        let property_desc = mapping
            .get_value(
                record,
                headers,
                mapping
                    .property_description_column
                    .as_ref()
                    .unwrap_or(&"".to_string()),
            )
            .unwrap_or("");
        let property_type = mapping
            .get_value(
                record,
                headers,
                mapping.type_column.as_ref().unwrap_or(&"".to_string()),
            )
            .unwrap_or("string"); // Default to string type
        let property_class = mapping
            .get_value(
                record,
                headers,
                mapping
                    .property_class_column
                    .as_ref()
                    .unwrap_or(&"".to_string()),
            )
            .unwrap_or("");
        let extra_items = mapping
            .extra_items
            .values()
            .map(|extra_item| {
                let value = mapping
                    .get_value(record, headers, &extra_item.column)
                    .unwrap_or("")
                    .to_string();
                let extra_item = extra_item.set_value(value);
                (extra_item.map_to.clone(), extra_item)
            })
            .collect::<HashMap<_, _>>();
        let result = RowValues {
            class_id,
            class_name: None,
            class_description: None,
            property_id: Some(property),
            property_name: Some(property_name),
            property_description: Some(property_desc),
            property_type: Some(property_type),
            property_class: Some(property_class),
            extra_items,
        };
        Ok(result)
    }
}

#[derive(Debug, Serialize)]
pub struct VocabularyColumnMapping {
    pub class_column: IdOpt,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class_label_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class_description_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property_column: Option<IdOpt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property_name_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property_description_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property_class_column: Option<String>,
    pub extra_items: HashMap<String, ExtraItem>,
}

impl VocabularyColumnMapping {
    pub fn basic_vocabulary_step() -> Self {
        Self {
            class_column: IdOpt::String("Class ID".to_string()),
            class_label_column: Some("Class Name".to_string()),
            class_description_column: Some("Class Description".to_string()),
            property_column: Some(IdOpt::String("Property ID".to_string())),
            property_name_column: Some("Property Name".to_string()),
            property_description_column: Some("Property Description".to_string()),
            type_column: Some("Type".to_string()),
            property_class_column: Some("Class Range".to_string()),
            extra_items: HashMap::new(),
        }
    }

    pub fn sub_class_vocabulary_step() -> Self {
        Self {
            class_column: IdOpt::String("Class ID".to_string()),
            class_label_column: Some("Class Name".to_string()),
            class_description_column: Some("Class Description".to_string()),
            property_column: None,
            property_name_column: None,
            property_description_column: None,
            type_column: None,
            property_class_column: None,
            extra_items: HashMap::new(),
        }
    }

    pub fn property_vocabulary_step() -> Self {
        Self {
            class_column: IdOpt::String("Class ID".to_string()),
            class_label_column: None,
            class_description_column: None,
            property_column: Some(IdOpt::String("Property ID".to_string())),
            property_name_column: Some("Property Name".to_string()),
            property_description_column: Some("Property Description".to_string()),
            type_column: Some("Type".to_string()),
            property_class_column: Some("Class Range".to_string()),
            extra_items: HashMap::new(),
        }
    }

    pub fn replace_class_id_with(&mut self, replace_id_with: &str) -> Result<(), ProcessorError> {
        tracing::info!("Replacing Class ID with: {}", replace_id_with);
        let original_id = if let IdOpt::String(s) = &self.class_column {
            s.clone()
        } else {
            panic!("Expected class_column to be a string");
        };
        let replace_id_with_trimmed = validate_column_identifier(replace_id_with.to_string())?;
        let replacement_id = match replace_id_with_trimmed.as_str() {
                "Class.ID" => {
                    return Err(ProcessorError::InvalidManifest(
                        "Cannot replace Class ID with Class.ID".to_string(),
                    ));
                },
                "Class.Name" => self.class_label_column.clone().ok_or(ProcessorError::InvalidManifest("Cannot replace Class ID with Class.Name if Class.Name does not exist".to_string()))?,
                "Class.Description" => self.class_description_column.clone().ok_or(ProcessorError::InvalidManifest("Cannot replace Class ID with Class.Description if Class.Description does not exist".to_string()))?,
                "Property.ID" => self.property_column.clone().ok_or(ProcessorError::InvalidManifest("Cannot replace Class ID with Property.ID if Property.ID does not exist".to_string()))?.final_iri(),
                "Property.Description" => self.property_description_column.clone().ok_or(ProcessorError::InvalidManifest("Cannot replace Class ID with Property.Description if Property.Description does not exist".to_string()))?,
                "Property.Type" => self.type_column.clone().ok_or(ProcessorError::InvalidManifest("Cannot replace Class ID with Property.Type if Property.Type does not exist".to_string()))?,
                "Property.TargetClass" => self.property_class_column.clone().ok_or(ProcessorError::InvalidManifest("Cannot replace Class ID with Property.TargetClass if Property.TargetClass does not exist".to_string()))?,
                _ => {
                    return Err(ProcessorError::InvalidManifest(
                        format!("Invalid replaceClassIdWith value: {}", replace_id_with),
                    ));
                }
            };
        self.class_column = IdOpt::ReplacementMap {
            original_id,
            replacement_id,
        };
        Ok(())
    }

    pub fn replace_property_id_with(
        &mut self,
        replace_id_with: &str,
    ) -> Result<(), ProcessorError> {
        tracing::info!("Replacing Property ID with: {}", replace_id_with);
        let original_id = match &self.property_column {
            Some(IdOpt::String(s)) => s.clone(),
            Some(IdOpt::ReplacementMap { original_id, .. }) => original_id.clone(),
            None => {
                return Err(ProcessorError::InvalidManifest(
                    "Cannot replace Property ID if Property ID column (or an override) does not exist".to_string(),
                ));
            }
        };
        let replace_id_with_trimmed = validate_column_identifier(replace_id_with.to_string())?;
        let replacement_id = match replace_id_with_trimmed.as_str() {
                "Class.ID" => {
                    return Err(ProcessorError::InvalidManifest(
                        "Cannot replace Property ID with Class.ID".to_string(),
                    ));
                },
                "Class.Name" => {
                    return Err(ProcessorError::InvalidManifest(
                        "Cannot replace Property ID with Class.Name".to_string(),
                    ));
                },
                "Class.Description" => {
                    return Err(ProcessorError::InvalidManifest(
                        "Cannot replace Property ID with Class.Description".to_string(),
                    ));
                }
                "Property.ID" => {
                    return Err(ProcessorError::InvalidManifest(
                        "Cannot replace Property ID with itself".to_string(),
                    ));
                }
                "Property.Name" => self.property_name_column.clone().ok_or(ProcessorError::InvalidManifest("Cannot replace Property ID with Property.Name if Property.Name does not exist".to_string()))?,
                "Property.Description" => self.property_description_column.clone().ok_or(ProcessorError::InvalidManifest("Cannot replace Property ID with Property.Description if Property.Description does not exist".to_string()))?,
                "Property.Type" => self.type_column.clone().ok_or(ProcessorError::InvalidManifest("Cannot replace Property ID with Property.Type if Property.Type does not exist".to_string()))?,
                "Property.TargetClass" => self.property_class_column.clone().ok_or(ProcessorError::InvalidManifest("Cannot replace Property ID with Property.TargetClass if Property.TargetClass does not exist".to_string()))?,
                _ => {
                    return Err(ProcessorError::InvalidManifest(
                        format!("Invalid replaceIdWith value: {}", replace_id_with),
                    ));
                }
            };
        self.property_column = Some(IdOpt::ReplacementMap {
            original_id,
            replacement_id,
        });
        Ok(())
    }

    pub fn handle_override(
        &mut self,
        override_: &ColumnOverride,
        mapping_type: &StepType,
    ) -> Result<(), ProcessorError> {
        let override_string = validate_column_identifier(override_.map_to.clone())?;
        match mapping_type {
            StepType::ModelStep(ModelStep::BasicVocabularyStep) => match override_string.as_str() {
                "Class.ID" => self.class_column = IdOpt::String(override_.column.clone()),
                "Class.Name" => self.class_label_column = Some(override_.column.clone()),
                "Class.Description" => {
                    self.class_description_column = Some(override_.column.clone())
                }
                "Property.ID" => {
                    self.property_column = Some(IdOpt::String(override_.column.clone()))
                }
                "Property.Name" => self.property_name_column = Some(override_.column.clone()),
                "Property.Description" => {
                    self.property_description_column = Some(override_.column.clone())
                }
                "Property.Type" => self.type_column = Some(override_.column.clone()),
                "Property.TargetClass" => {
                    self.property_class_column = Some(override_.column.clone())
                }
                _ => {
                    return Err(ProcessorError::InvalidManifest(format!(
                    "Invalid override mapTo value for BasicVocabularyStep: {}. Overrides must be one of the following: Class.ID, Class.Description, Property.ID, Property.Description, Property.Type, Property.TargetClass. If you want to specify an extraItem, use the extraItems field in the manifest",
                    override_.map_to
                )));
                }
            },
            StepType::ModelStep(ModelStep::SubClassVocabularyStep) => {
                match override_string.as_str() {
                    "Class.ID" => self.class_column = IdOpt::String(override_.column.clone()),
                    "Class.Name" => self.class_label_column = Some(override_.column.clone()),
                    "Class.Description" => {
                        self.class_description_column = Some(override_.column.clone())
                    }
                    _ => {
                        return Err(ProcessorError::InvalidManifest(format!(
                        "Invalid override mapTo value for SubClassVocabularyStep: {}. Overrides must be one of the following: Class.ID, Class.Description",
                        override_.map_to
                    )));
                    }
                }
            }
            _ => {
                return Err(ProcessorError::InvalidManifest(format!(
                    "Invalid StepType for handle_override: {:?}",
                    mapping_type
                )));
            }
        }
        Ok(())
    }

    pub fn get_value<'a>(
        &self,
        record: &'a csv::StringRecord,
        headers: &csv::StringRecord,
        column: &str,
    ) -> Option<&'a str> {
        headers
            .iter()
            .position(|h| h == column)
            .and_then(|i| record.get(i))
    }

    pub fn get_id_value(
        &self,
        record: &csv::StringRecord,
        headers: &csv::StringRecord,
        column: &IdOpt,
    ) -> Option<IdOpt> {
        match column {
            IdOpt::String(s) => self
                .get_value(record, headers, s)
                .map(|value| IdOpt::String(value.to_string())),
            IdOpt::ReplacementMap {
                original_id,
                replacement_id,
            } => {
                let original_id_value = self.get_value(record, headers, original_id)?;
                let replacement_id_value = self.get_value(record, headers, replacement_id)?;
                Some(IdOpt::ReplacementMap {
                    original_id: original_id_value.to_string(),
                    replacement_id: replacement_id_value.to_string(),
                })
            }
        }
    }

    pub fn validate_headers(
        &self,
        headers: &StringRecord,
        is_strict: bool,
    ) -> Result<(), ProcessorError> {
        // Required columns
        let required_columns = vec![&self.class_column];

        // Optional columns
        let optional_columns = [
            self.class_label_column.as_ref(),
            self.class_description_column.as_ref(),
            self.property_column.as_ref().map(|id_opt| match id_opt {
                IdOpt::String(s) => s,
                IdOpt::ReplacementMap { original_id, .. } => original_id,
            }),
            self.property_description_column.as_ref(),
            self.type_column.as_ref(),
            self.property_class_column.as_ref(),
        ];

        // Check required columns
        for column in required_columns {
            match column {
                IdOpt::String(s) => {
                    if !headers.iter().any(|h| h == s) {
                        if is_strict {
                            return Err(ProcessorError::Processing(format!(
                                "Required column '{}' not found in CSV headers",
                                s
                            )));
                        } else {
                            tracing::warn!("Required column '{}' not found in CSV headers, some data may be missing", s);
                        }
                    }
                }
                IdOpt::ReplacementMap {
                    original_id,
                    replacement_id,
                } => {
                    if !headers.iter().any(|h| h == original_id) {
                        return Err(ProcessorError::Processing(format!(
                            "Required column '{}' not found in CSV headers",
                            original_id
                        )));
                    }
                    if !headers.iter().any(|h| h == replacement_id) {
                        return Err(ProcessorError::Processing(format!(
                            "Required column '{}' not found in CSV headers",
                            replacement_id
                        )));
                    }
                }
            }
        }

        // Check optional columns only in strict mode
        if is_strict {
            for column in optional_columns.iter().flatten() {
                if !headers.iter().any(|h| h == *column) {
                    tracing::warn!("Optional column '{}' not found in CSV headers", column);
                }
            }
        }

        Ok(())
    }
}
