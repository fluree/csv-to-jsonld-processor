use crate::error::ProcessorError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ColumnOverride {
    pub column: String,
    #[serde(rename = "mapTo")]
    pub map_to: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum OnEntity {
    #[serde(rename = "CLASS")]
    Class,
    #[serde(rename = "PROPERTY")]
    Property,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExtraItem {
    pub column: String,
    #[serde(rename = "mapTo")]
    pub map_to: String,
    #[serde(rename = "onEntity")]
    pub on_entity: OnEntity,
    pub value: Option<String>,
}

impl ExtraItem {
    pub fn set_value(&self, value: String) -> Self {
        Self {
            column: self.column.clone(),
            map_to: self.map_to.clone(),
            on_entity: self.on_entity.clone(),
            value: Some(value),
        }
    }
}

// #[derive(Debug, Serialize)]
// pub struct VocabularyColumnMapping {
//     pub class_column: String,
//     pub class_description_column: String,
//     pub property_column: String,
//     pub property_description_column: String,
//     pub type_column: String,
//     pub property_class_column: String,
//     pub extra_items: HashMap<String, String>,
// }

// impl VocabularyColumnMapping {
//     pub fn from_headers(
//         headers: &csv::StringRecord,
//         overrides: &[ColumnOverride],
//         extra_items: &[ExtraItem],
//         is_strict: bool,
//     ) -> Result<Self, ProcessorError> {
//         // Default column names
//         let mut mapping = Self {
//             class_column: "Class".to_string(),
//             class_description_column: "Class Description".to_string(),
//             property_column: "Property Name".to_string(),
//             property_description_column: "Property Description".to_string(),
//             type_column: "Type".to_string(),
//             property_class_column: "Class Range".to_string(), // The second "Class" column
//             extra_items: HashMap::new(),
//         };

//         // Apply any overrides from the manifest
//         for override_ in overrides {
//             mapping.handle_override(override_)?;
//         }

//         for extra_item in extra_items {
//             mapping
//                 .extra_items
//                 .insert(extra_item.column.clone(), extra_item.map_to.clone());
//         }

//         // Verify all required columns exist
//         let required_columns = [
//             (&mapping.class_column, "Class"),
//             (&mapping.property_column, "Property Name"),
//             (&mapping.type_column, "Type"),
//         ];

//         for (column, name) in required_columns.iter() {
//             if !headers.iter().any(|h| h == column.as_str()) {
//                 return Err(ProcessorError::Processing(format!(
//                     "Required column '{}' not found in CSV headers",
//                     name
//                 )));
//             }
//         }

//         let mut json_value_of_mapping = serde_json::to_value(&mapping).unwrap();
//         let json_object_of_mapping = json_value_of_mapping.as_object_mut().unwrap();
//         for (extra_item, map_to) in mapping.extra_items.iter() {
//             json_object_of_mapping.insert(
//                 map_to.to_string(),
//                 serde_json::Value::String(extra_item.clone()),
//             );
//         }
//         for (concept_to_extract, expected_csv_header) in json_object_of_mapping.iter() {
//             if concept_to_extract == "extra_items" {
//                 continue;
//             }
//             if !headers
//                 .iter()
//                 .any(|h| h == expected_csv_header.as_str().unwrap())
//             {
//                 if is_strict {
//                     return Err(ProcessorError::Processing(format!(
//                         "Column '{}' not found in CSV headers. If this is acceptable, run again without --strict",
//                         expected_csv_header
//                     )));
//                 } else {
//                     tracing::warn!(
//                         "Column '{}' not found in CSV headers. If this is not expected, check your CSV file for typos or missing columns, or update the manifest to match the CSV file",
//                         expected_csv_header
//                     );
//                 }
//             }
//         }

//         Ok(mapping)
//     }

//     pub fn handle_override(&mut self, override_: &ColumnOverride) -> Result<(), ProcessorError> {
//         match override_.map_to.as_str() {
//             "Class.ID" => self.class_column = override_.column.clone(),
//             "Class.Description" => self.class_description_column = override_.column.clone(),
//             "Property.ID" => self.property_column = override_.column.clone(),
//             "Property.Description" => self.property_description_column = override_.column.clone(),
//             "Property.Type" => self.type_column = override_.column.clone(),
//             "Property.TargetClass" => self.property_class_column = override_.column.clone(),
//             _ => {
//                 return Err(ProcessorError::Processing(format!(
//                     "Invalid override mapTo value: {}. Overrides must be one of the following: Class.ID, Class.Description, Property.ID, Property.Description, Property.Type, Property.TargetClass. If you want to specify an extraItem, use the extraItems field in the manifest",
//                     override_.map_to
//                 )));
//             }
//         }
//         Ok(())
//     }

//     pub fn get_value<'a>(
//         &self,
//         record: &'a csv::StringRecord,
//         headers: &csv::StringRecord,
//         column: &str,
//     ) -> Option<&'a str> {
//         headers
//             .iter()
//             .position(|h| h == column)
//             .and_then(|i| record.get(i))
//     }
// }
