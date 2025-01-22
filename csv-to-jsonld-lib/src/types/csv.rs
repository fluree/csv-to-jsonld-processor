use crate::error::ProcessorError;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{
    fmt::{self},
    str::FromStr,
};

use super::VocabularyTerm;

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
#[allow(clippy::upper_case_acronyms)]
pub enum PropertyDatatype {
    ID,
    URI(Option<String>),
    Picklist(Option<String>),
    String,
    Decimal,
    Integer,
    Date,
    Boolean,
}

impl FromStr for PropertyDatatype {
    type Err = ProcessorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "primary key identifier" | "@id" => Ok(PropertyDatatype::ID),
            "foreign key reference" | "uri" => Ok(PropertyDatatype::URI(None)),
            "picklist" => Ok(PropertyDatatype::Picklist(None)),
            "string" | "" => Ok(PropertyDatatype::String),
            "float" => Ok(PropertyDatatype::Decimal),
            "integer" => Ok(PropertyDatatype::Integer),
            "date/time" | "date" => Ok(PropertyDatatype::Date),
            "boolean" => Ok(PropertyDatatype::Boolean),
            _ => Err(ProcessorError::Processing(format!(
                "Invalid CSV datatype: {} [Expected: @id, URI, String, Float, Integer, Date]",
                s.trim().to_lowercase().as_str()
            ))),
        }
    }
}

// Implement Deserialize
impl<'de> Deserialize<'de> for PropertyDatatype {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PropertyDatatypeVisitor;

        impl<'de> Visitor<'de> for PropertyDatatypeVisitor {
            type Value = PropertyDatatype;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str(
                    "a valid CSV datatype string: @id, URI, String, Float, Integer, Date",
                )
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                PropertyDatatype::from_str(value).map_err(|_| {
                    E::custom(format!(
                        "Invalid CSV datatype: {} [Expected: @id, URI, String, Float, Integer, Date]",
                        value.trim().to_lowercase().as_str()
                    ))
                })
            }
        }

        deserializer.deserialize_str(PropertyDatatypeVisitor)
    }
}

// Implement Serialize
impl Serialize for PropertyDatatype {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let xsd_value = match self {
            PropertyDatatype::ID => "xsd:anyURI",
            PropertyDatatype::URI(string_option) => match string_option {
                Some(string) => string,
                None => "xsd:anyURI",
            },
            PropertyDatatype::Picklist(string_option) => match string_option {
                Some(string) => string,
                None => "xsd:anyURI",
            },
            PropertyDatatype::String => "xsd:string",
            PropertyDatatype::Decimal => "xsd:decimal",
            PropertyDatatype::Integer => "xsd:integer",
            PropertyDatatype::Date => "xsd:date",
            PropertyDatatype::Boolean => "xsd:boolean",
        };
        serializer.serialize_str(xsd_value)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Hash, Eq)]
pub struct Header {
    pub name: String,
    pub datatype: PropertyDatatype,
    pub is_label_header: bool,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            name: String::new(),
            datatype: PropertyDatatype::String,
            is_label_header: false,
        }
    }
}

impl Header {
    pub fn set_is_label_header(&mut self, is_label_header: bool) {
        self.is_label_header = is_label_header;
    }
}

impl TryFrom<&VocabularyTerm> for Header {
    type Error = ProcessorError;

    fn try_from(term: &VocabularyTerm) -> Result<Self, Self::Error> {
        let vec_range = term.range.clone().ok_or_else(|| {
            ProcessorError::Processing(format!("Vocabulary term '{}' has an empty range", term.id))
        })?;
        let datatype = vec_range.first().ok_or_else(|| {
            ProcessorError::Processing(format!("Vocabulary term '{}' has an empty range", term.id))
        })?;
        let name = term.label.clone().ok_or_else(|| {
            ProcessorError::Processing(format!(
                "Vocabulary term {} must have a label for processing CSV headers",
                term.id
            ))
        })?;
        Ok(Self {
            name,
            datatype: datatype.clone(),
            is_label_header: false,
        })
    }
}

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
pub struct PivotColumn {
    #[serde(rename = "instanceType")]
    pub instance_type: String,
    #[serde(rename = "newRelationshipProperty")]
    pub new_relationship_property: String,
    pub columns: Vec<String>,
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
