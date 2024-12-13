use csv::StringRecord;
use serde::{Serialize, Serializer};
use std::collections::HashMap;

use crate::error::ProcessorError;
use crate::manifest::{ModelStep, StepType};
use crate::types::{ColumnOverride, ExtraItem, IdOpt};
use crate::utils::validate_column_identifier;
use crate::ImportStep;

pub struct RowValues<'a> {
    pub class_id: IdOpt,
    pub class_name: &'a str,
    pub class_description: &'a str,
    pub property_id: Option<&'a str>,
    pub property_description: Option<&'a str>,
    pub property_type: Option<&'a str>,
    pub property_class: Option<&'a str>,
    pub extra_items: HashMap<String, ExtraItem>,
}

#[derive(Debug)]
pub struct MappingConfig {
    pub type_: StepType,
    pub column_mapping: VocabularyColumnMapping,
}

impl MappingConfig {
    pub fn extract_values<'a>(
        &self,
        record: &'a StringRecord,
        headers: &StringRecord,
    ) -> Result<RowValues<'a>, ProcessorError> {
        match self.type_ {
            StepType::ModelStep(ModelStep::BasicVocabularyStep) => {
                Ok(self.extract_basic_vocabulary_values(record, headers)?)
            }
            StepType::ModelStep(ModelStep::SubClassVocabularyStep) => {
                Ok(self.extract_sub_class_vocabulary_values(record, headers)?)
            }
            _ => Err(ProcessorError::InvalidManifest(
                "Invalid StepType for MappingConfig".to_string(),
            )),
        }
    }

    fn extract_basic_vocabulary_values<'a>(
        &self,
        record: &'a StringRecord,
        headers: &StringRecord,
    ) -> Result<RowValues<'a>, ProcessorError> {
        let mapping = &self.column_mapping;
        let class_id = mapping
            .get_id_value(&record, &headers, &mapping.class_column)
            .ok_or_else(|| ProcessorError::Processing("Missing Class ID column".into()))?;
        let class_name = mapping
            .get_value(&record, &headers, &mapping.class_label_column)
            .ok_or_else(|| ProcessorError::Processing("Missing Class column".into()))?;
        let class_desc = mapping
            .get_value(&record, &headers, &mapping.class_description_column)
            .ok_or_else(|| ProcessorError::Processing("Missing Class Description column".into()))?;
        let property = mapping
            .get_value(
                &record,
                &headers,
                &mapping.property_column.as_ref().unwrap(),
            )
            .ok_or_else(|| ProcessorError::Processing("Missing Property column".into()))?;
        let property_desc = mapping
            .get_value(
                &record,
                &headers,
                &mapping.property_description_column.as_ref().unwrap(),
            )
            .ok_or_else(|| {
                ProcessorError::Processing("Missing Property Description column".into())
            })?;
        let property_type = mapping
            .get_value(&record, &headers, &mapping.type_column.as_ref().unwrap())
            .ok_or_else(|| ProcessorError::Processing("Missing Type column".into()))?;
        let property_class = mapping
            .get_value(
                &record,
                &headers,
                &mapping.property_class_column.as_ref().unwrap(),
            )
            .unwrap_or("");
        let extra_items = mapping
            .extra_items
            .iter()
            .map(|(_, extra_item)| {
                let value = mapping
                    .get_value(&record, &headers, &extra_item.column)
                    .unwrap_or("")
                    .to_string();
                let extra_item = extra_item.set_value(value);
                (extra_item.map_to.clone(), extra_item)
            })
            .collect::<HashMap<_, _>>();
        let result = RowValues {
            class_id,
            class_name,
            class_description: class_desc,
            property_id: Some(property),
            property_description: Some(property_desc),
            property_type: Some(property_type),
            property_class: Some(property_class),
            extra_items,
        };
        Ok(result)
    }

    fn extract_sub_class_vocabulary_values<'a>(
        &self,
        record: &'a StringRecord,
        headers: &StringRecord,
    ) -> Result<RowValues<'a>, ProcessorError> {
        let mapping = &self.column_mapping;
        let class_id = mapping
            .get_id_value(&record, &headers, &mapping.class_column)
            .ok_or_else(|| ProcessorError::Processing("Missing Class ID column".into()))?;
        let class_name = mapping
            .get_value(&record, &headers, &mapping.class_label_column)
            .ok_or_else(|| ProcessorError::Processing("Missing Class column".into()))?;
        let class_desc = mapping
            .get_value(&record, &headers, &mapping.class_description_column)
            .ok_or_else(|| ProcessorError::Processing("Missing Class Description column".into()))?;
        let extra_items = mapping
            .extra_items
            .iter()
            .map(|(_, extra_item)| {
                let value = mapping
                    .get_value(&record, &headers, &extra_item.column)
                    .unwrap_or("")
                    .to_string();
                let extra_item = extra_item.set_value(value);
                (extra_item.map_to.clone(), extra_item)
            })
            .collect::<HashMap<_, _>>();

        let result = RowValues {
            class_id,
            class_name,
            class_description: class_desc,
            property_id: None,
            property_description: None,
            property_type: None,
            property_class: None,
            extra_items,
        };
        Ok(result)
    }
}

#[derive(Debug, Serialize)]
pub struct VocabularyColumnMapping {
    pub class_column: IdOpt,
    pub class_label_column: String,
    pub class_description_column: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property_column: Option<String>,
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
            class_label_column: "Class Name".to_string(),
            class_description_column: "Class Description".to_string(),
            property_column: Some("Property Name".to_string()),
            property_description_column: Some("Property Description".to_string()),
            type_column: Some("Type".to_string()),
            property_class_column: Some("Class Range".to_string()),
            extra_items: HashMap::new(),
        }
    }

    pub fn sub_class_vocabulary_step() -> Self {
        Self {
            class_column: IdOpt::String("Class ID".to_string()),
            class_label_column: "Class Name".to_string(),
            class_description_column: "Class Description".to_string(),
            property_column: None,
            property_description_column: None,
            type_column: None,
            property_class_column: None,
            extra_items: HashMap::new(),
        }
    }

    pub fn replace_id_with(&mut self, replace_id_with: &str) -> Result<(), ProcessorError> {
        tracing::info!("Replacing ID with: {}", replace_id_with);
        let original_id = if let IdOpt::String(s) = &self.class_column {
            s.clone()
        } else {
            panic!("Expected class_column to be a string");
        };
        let replace_id_with_trimmed = validate_column_identifier(replace_id_with.to_string())?;
        let replacement_id = match replace_id_with_trimmed.as_str() {
                "Class.ID" => {
                    return Err(ProcessorError::InvalidManifest(
                        "Cannot replace ID with Class.ID".to_string(),
                    ));
                },
                "Class.Name" => self.class_label_column.clone(),
                "Class.Description" => self.class_description_column.clone(),
                "Property.ID" => self.property_column.clone().ok_or(ProcessorError::InvalidManifest("Cannot replace ID with Property.ID if Property.ID does not exist".to_string()))?,
                "Property.Description" => self.property_description_column.clone().ok_or(ProcessorError::InvalidManifest("Cannot replace ID with Property.Description if Property.Description does not exist".to_string()))?,
                "Property.Type" => self.type_column.clone().ok_or(ProcessorError::InvalidManifest("Cannot replace ID with Property.Type if Property.Type does not exist".to_string()))?,
                "Property.TargetClass" => self.property_class_column.clone().ok_or(ProcessorError::InvalidManifest("Cannot replace ID with Property.TargetClass if Property.TargetClass does not exist".to_string()))?,
                _ => {
                    return Err(ProcessorError::InvalidManifest(
                        format!("Invalid replaceIdWith value: {}", replace_id_with),
                    ));
                }
            };
        self.class_column = IdOpt::ReplacementMap {
            original_id,
            replacement_id,
        };
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
                "Class.Name" => self.class_label_column = override_.column.clone(),
                "Class.Description" => self.class_description_column = override_.column.clone(),
                "Property.ID" => self.property_column = Some(override_.column.clone()),
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
                    "Class.Name" => self.class_label_column = override_.column.clone(),
                    "Class.Description" => self.class_description_column = override_.column.clone(),
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

    pub fn get_id_value<'a>(
        &self,
        record: &'a csv::StringRecord,
        headers: &csv::StringRecord,
        column: &IdOpt,
    ) -> Option<IdOpt> {
        match column {
            IdOpt::String(s) => match self.get_value(record, headers, s) {
                Some(value) => Some(IdOpt::String(value.to_string())),
                None => None,
            },
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
}
