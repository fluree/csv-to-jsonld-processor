use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use crate::error::ProcessorError;
use crate::manifest::ImportStep;
use crate::types::{Header, IdOpt, JsonLdInstance, PivotColumn, PropertyDatatype, VocabularyMap};
use crate::utils::DATE_FORMATS;
use crate::Manifest;

pub struct InstanceProcessor {
    manifest: Arc<Manifest>,
    instances: HashMap<String, JsonLdInstance>,
    vocabulary: Option<VocabularyMap>,
    is_strict: bool,
    ignore: HashMap<String, Vec<String>>,
}

impl InstanceProcessor {
    pub fn new(manifest: Arc<Manifest>, is_strict: bool) -> Self {
        let ignore = manifest
            .instances
            .sequence
            .iter()
            .fold(HashMap::new(), |mut acc, step| {
                if let Some(ignores) = &step.ignore {
                    acc.insert(step.instance_type.clone(), ignores.clone());
                }
                acc
            });
        Self {
            manifest,
            instances: HashMap::new(),
            vocabulary: None,
            is_strict,
            ignore,
        }
    }

    pub fn set_vocabulary(&mut self, vocabulary: VocabularyMap) {
        self.vocabulary = Some(vocabulary);
    }

    pub fn get_vocabulary(&mut self) -> VocabularyMap {
        self.vocabulary.take().unwrap()
    }

    pub fn get_instances(&self) -> &HashMap<String, JsonLdInstance> {
        &self.instances
    }

    fn validate_headers(
        &self,
        headers: &[String],
        class_type: &str,
        identifier_label: &str,
        pivot_columns: Option<&Vec<PivotColumn>>,
    ) -> Result<Vec<Option<Header>>, ProcessorError> {
        let valid_labels = self.get_valid_property_labels(class_type, pivot_columns);

        let mut unknown_headers = Vec::new();
        let ignorable_headers = match self.ignore.get(class_type) {
            Some(headers) => headers.as_slice(),
            None => &[],
        };

        let mut final_headers = Vec::new();

        for header in headers {
            // Skip if it's the identifier column
            if header == identifier_label {
                let final_header = Header {
                    name: header.clone(),
                    datatype: PropertyDatatype::ID,
                };
                final_headers.push(Some(final_header));
                continue;
            }

            let final_header_candidate = valid_labels.iter().find(|label| &label.name == header);

            if final_header_candidate.is_none() && !ignorable_headers.contains(header) {
                tracing::debug!(
                    "Unknown column found in CSV for class '{}': {}",
                    class_type,
                    header
                );
                tracing::debug!("Valid labels: {:#?}", valid_labels);
                unknown_headers.push(header.clone());
            } else if final_header_candidate.is_some() {
                final_headers.push(Some(final_header_candidate.unwrap().clone()));
            } else if ignorable_headers.contains(header) {
                final_headers.push(None);
            }
        }

        if !unknown_headers.is_empty() {
            let message = format!(
                "Unknown columns found in CSV for class '{}': {:?}. These columns do not correspond to any properties defined in the vocabulary.",
                class_type, unknown_headers
            );

            if self.is_strict {
                return Err(ProcessorError::Processing(message));
            } else {
                tracing::warn!("{}", message);
            }
        }

        Ok(final_headers)
    }

    fn get_valid_property_labels(
        &self,
        class_type: &str,
        pivot_columns: Option<&Vec<PivotColumn>>,
    ) -> HashSet<Header> {
        let mut valid_labels: HashSet<Option<Header>> = HashSet::new();
        let vocab = self.vocabulary.as_ref().unwrap();
        let class_iri = format!("{}{}", self.manifest.model.base_iri, class_type);

        if let Some(pivot_columns) = pivot_columns {
            for pivot_column in pivot_columns {
                // In this case, we care about the instance type defined on the pivot column rather than the class for the wholes spreadsheet step
                let pivot_class = pivot_column.instance_type.to_string();
                let pivot_class_iri = IdOpt::String(pivot_class.clone())
                    .with_base_iri(self.manifest.model.base_iri.as_str());
                if let Some(class) = vocab.classes.values().find(|c| c.id == pivot_class_iri) {
                    tracing::debug!("[get_valid_property_labels] Matched pivot class: {:?} with class iri: {:?}", pivot_class_iri, class);
                    if let Some(props) = &class.range {
                        for prop_iri in props {
                            // Get property label from vocabulary
                            if let Some(prop) = vocab
                                .properties
                                .values()
                                // .find(|p| p.id.final_iri() == *prop_iri)
                                .find(|p| match prop_iri {
                                    PropertyDatatype::URI(Some(iri)) => p.id.final_iri() == *iri,
                                    _ => false,
                                })
                            {
                                tracing::debug!(
                                    "[get_valid_property_labels] Found property: {:#?}",
                                    prop
                                );
                                tracing::debug!(
                                    "[get_valid_property_labels] Matches pivot column: {:?}",
                                    pivot_column
                                );
                                valid_labels.insert(Header::try_from(prop).ok());
                            }
                        }
                    }
                }
            }
        }

        // Get properties from class's rdfs:range
        if let Some(class) = vocab
            .classes
            .values()
            .find(|c| c.id.final_iri() == class_iri)
        {
            if let Some(props) = &class.range {
                for prop_iri in props {
                    // Get property label from vocabulary
                    if let Some(prop) = vocab.properties.values().find(|p| match prop_iri {
                        PropertyDatatype::URI(Some(iri)) => p.id.final_iri() == *iri,
                        _ => false,
                    }) {
                        // valid_labels.insert(prop.label.clone());
                        valid_labels.insert(Header::try_from(prop).ok());
                    }
                }
            }
        }

        // Get properties that have this class in their rdfs:domain
        for prop in vocab.properties.values() {
            if let Some(domains) = &prop.domain {
                if domains.contains(&class_iri) {
                    valid_labels.insert(Header::try_from(prop).ok());
                }
            }
        }

        // drain / filter any entries in valid labels that are None
        let valid_labels: HashSet<Header> = valid_labels
            .into_iter()
            .flatten()
            .collect();

        valid_labels
    }

    // fn drop_ignore(
    //     &self,
    //     headers: &mut [String],
    //     class_type: &str,
    //     identifier_label: &str,
    // ) -> Vec<String> {
    //     let ignorable_headers = match self.ignore.get(class_type) {
    //         Some(ignorable_headers) => ignorable_headers.as_slice(),
    //         None => &[],
    //     };

    //     headers
    //         .iter()
    //         .map(|h| {
    //             if !ignorable_headers.contains(h) && *h != identifier_label {
    //                 h.clone()
    //             } else {
    //                 "".to_string()
    //             }
    //         })
    //         .collect()
    // }

    pub async fn process_simple_instance(
        &mut self,
        step: &ImportStep,
        instance_path: &str,
    ) -> Result<(), ProcessorError> {
        // Get the class type from the manifest
        let class_type = step.instance_type.clone();

        // Get the vocabulary map
        let vocab = self.vocabulary.as_ref().ok_or_else(|| {
            ProcessorError::Processing("Vocabulary must be set before processing instances".into())
        })?;

        tracing::debug!("Getting identifier label for class '{}'", class_type);

        // Get the identifier property for this class
        let identifier_label = vocab.get_identifier_label(&class_type).ok_or_else(|| {
            tracing::debug!("[Error finding identifier label] {:#?}", vocab.identifiers.keys());
            ProcessorError::Processing(format!(
                "No identifier property found for class '{}'. To import instances of this class, your data model CSVs must indicate which column of the instance data will be used as the identifier (\"@id\") for instances of this class.",
                class_type
            ))
        })?;

        let file_path = PathBuf::from(instance_path).join(&step.path);
        tracing::debug!("Reading instance data from {:?}", file_path);

        let mut rdr = csv::Reader::from_path(&file_path).map_err(|e| {
            ProcessorError::Processing(format!(
                "Failed to read CSV @ {}: {}",
                &file_path.to_string_lossy(),
                e
            ))
        })?;

        // Read headers first and collect them into a Vec
        let headers: Vec<String> = rdr
            .headers()
            .map_err(|e| ProcessorError::Processing(format!("Failed to read CSV headers: {}", e)))?
            .iter()
            .map(|h| h.to_string())
            .collect();

        // let pivot_columns =
        //     self.validate_pivot_columns(step.pivot_columns.as_ref(), &class_type)?;
        if let Some(pivot_columns) = &step.pivot_columns {
            self.validate_pivot_columns(pivot_columns.iter().collect(), &class_type)?;
        };

        // Validate headers against vocabulary
        let headers = self.validate_headers(
            &headers,
            &class_type,
            identifier_label,
            step.pivot_columns.as_ref(),
        )?;

        // Find the identifier column index
        let id_column_index = headers
            .iter()
            // .position(|h| &h.name == identifier_label)
            .position(|h| {
                h.as_ref()
                    .map(|h| h.name == *identifier_label)
                    .unwrap_or(false)
            })
            .ok_or_else(|| {
                ProcessorError::Processing(format!(
                    "Identifier column '{}' not found in headers",
                    identifier_label
                ))
            })?;

        // let headers = self.drop_ignore(&mut headers, &class_type, identifier_label);

        tracing::debug!("[Process_simple_instance] Headers: {:#?}", headers);

        // Process each row
        for result in rdr.records() {
            let record = result.map_err(|e| {
                ProcessorError::Processing(format!("Failed to read CSV record: {}", e))
            })?;

            // Get the identifier value
            let id = record
                .get(id_column_index)
                .ok_or_else(|| ProcessorError::Processing("Missing identifier value".into()))?;

            let mut properties = serde_json::Map::new();

            if headers.len() != record.len() {
                tracing::error!("RECORD: {:#?}", record);
                tracing::error!("HEADERS: {:#?}", headers);
                return Err(ProcessorError::Processing(format!(
                    "Row has different number of columns than headers: RECORD: {} HEADERS: {}",
                    record.len(),
                    headers.len()
                )));
            }

            // Map CSV columns to JSON-LD properties
            for (i, header) in headers.iter().enumerate() {
                // if header.is_none() {
                //     continue;
                // }
                if let Some(header) = header {
                    if let Some(value) = record.get(i) {
                        // Skip empty values
                        if !value.is_empty() {
                            let is_pivot_header =
                                step.pivot_columns.as_ref().and_then(|pivot_columns| {
                                    pivot_columns.iter().find(|pivot_column| {
                                        pivot_column.columns.contains(&header.name)
                                    })
                                });

                            let final_value = match header.datatype {
                                PropertyDatatype::ID => {
                                    serde_json::Value::String(value.to_string())
                                }
                                PropertyDatatype::Date => {
                                    let date = DATE_FORMATS
                                        .iter()
                                        .find_map(|fmt| {
                                            chrono::NaiveDate::parse_from_str(value, fmt).ok()
                                        })
                                        .ok_or_else(|| {
                                            ProcessorError::Processing(format!(
                                                "Failed to parse date {}",
                                                value
                                            ))
                                        })?;
                                    // let date = {
                                    //     let mut date_result = None;
                                    //     for format in DATE_FORMATS {
                                    //         if let Ok(date) =
                                    //             chrono::NaiveDate::parse_from_str(value, format)
                                    //         {
                                    //             date_result = Some(date);
                                    //             break;
                                    //         }
                                    //     }
                                    //     date_result.ok_or_else(|| {
                                    //         ProcessorError::Processing(format!(
                                    //             "Failed to parse date {}",
                                    //             value
                                    //         ))
                                    //     })?
                                    // };
                                    serde_json::Value::String(date.format("%Y-%m-%d").to_string())
                                }
                                PropertyDatatype::Integer => {
                                    let cleaned_value = value.replace(['$', '%', ','], "");
                                    if let Ok(num) = cleaned_value.parse::<i64>() {
                                        serde_json::Value::Number(serde_json::Number::from(num))
                                    } else {
                                        serde_json::Value::String(cleaned_value.to_string())
                                    }
                                }
                                PropertyDatatype::Decimal => {
                                    let cleaned_value = value.replace(['$', '%', ','], "");
                                    if let Ok(num) = cleaned_value.parse::<f64>() {
                                        serde_json::Value::Number(
                                            serde_json::Number::from_f64(num).unwrap(),
                                        )
                                    } else {
                                        serde_json::Value::String(cleaned_value.to_string())
                                    }
                                }
                                PropertyDatatype::String => {
                                    serde_json::Value::String(value.to_string())
                                }
                                PropertyDatatype::URI(_) => {
                                    let class_match = self
                                        .vocabulary
                                        .as_ref()
                                        .unwrap()
                                        .classes
                                        .iter()
                                        .find_map(|(id, _)| match id {
                                            IdOpt::String(string_id) if string_id == value => Some(id.clone()),
                                            IdOpt::ReplacementMap { original_id, .. } if original_id == value => {
                                                Some(id.clone())
                                            }
                                            _ => None,
                                        });
                                    
                                    if let Some(class_id) = class_match {
                                        tracing::debug!("Found class match: {:#?} for value: {}", class_id, value);
                                        serde_json::Value::String(class_id.normalize().to_pascal_case().final_iri())
                                    } else {
                                        serde_json::Value::String(value.to_string())
                                    }
                                }
                            };

                            if let Some(pivot_column_match) = is_pivot_header {
                                let pivot_property_entry = properties
                                    .entry(pivot_column_match.new_relationship_property.clone())
                                    .or_insert_with(|| {
                                        let id = uuid::Uuid::new_v4().to_string();
                                        let mut new_map = serde_json::Map::new();
                                        new_map.insert("@id".to_string(), serde_json::Value::String(id));
                                        serde_json::Value::Object(new_map)
                                    });
                                let id = pivot_property_entry.get("@id").unwrap().as_str().unwrap();
                                // Handle numeric values
                                let mut properties = serde_json::Map::new();
                                properties.insert(header.name.clone(), final_value);

                                let new_instance = JsonLdInstance {
                                    id: id.to_string(),
                                    type_: vec![IdOpt::String(pivot_column_match.instance_type.clone())],
                                    properties
                                };

                                match self.instances.entry(id.to_string()) {
                                    Entry::Occupied(mut entry) => {
                                        entry.get_mut().update_with(new_instance)?;
                                    },
                                    Entry::Vacant(entry) => {
                                        entry.insert(new_instance);
                                    },
                                };

                            } else {
                                // Handle numeric values
                                properties.insert(header.name.clone(), final_value);
                            }
                        }
                    }
                } else {
                    continue;
                }
            }

            let instance = JsonLdInstance {
                id: id.to_string(),
                type_: vec![IdOpt::String(class_type.clone())],
                properties,
            };

            tracing::debug!("Updating or inserting instance: {:#?}", instance);

            self.update_or_insert_instance(instance)?;
        }

        Ok(())
    }

    pub fn validate_pivot_columns(
        &self,
        pivot_columns: Vec<&PivotColumn>,
        base_csv_class: &str,
    ) -> Result<(), ProcessorError> {
        let vocab = self.vocabulary.as_ref().unwrap();

        for pivot_column in pivot_columns {
            // let base_csv_class = class_type.as_str();
            let pivot_class = pivot_column.instance_type.to_string();
            // let pivot_class_iri = format!("{}{}", self.manifest.model.base_iri, pivot_class);
            let pivot_class_iri = IdOpt::String(pivot_class.clone())
                .with_base_iri(self.manifest.model.base_iri.as_str());

            let pivot_column_ref_property = pivot_column.new_relationship_property.as_str();

            if let Some(base_class_vocab_def) = vocab
                .classes
                .values()
                .find(|c| c.label == Some(base_csv_class.to_string()))
            {
                if let Some(range_props) = &base_class_vocab_def.range {
                    let base_class_ref_property = vocab
                        .properties
                        .values()
                        .find(|prop| {
                            range_props.iter().any(|p_datatype| match p_datatype {
                                PropertyDatatype::URI(Some(iri)) => prop.id.final_iri() == *iri,
                                _ => false,
                            }) && prop.label == Some(pivot_column_ref_property.to_string())
                                && prop.range.is_some()
                                && prop.range.clone().unwrap().iter().any(|r| match r {
                                    PropertyDatatype::URI(Some(iri)) => pivot_class.clone() == *iri,
                                    _ => false,
                                })
                        })
                        .ok_or(ProcessorError::Processing(format!(
                            "Base class {} has no property defined for referencing pivot class {}",
                            base_csv_class, pivot_class
                        )))?;
                    if base_class_ref_property.label != Some(pivot_column_ref_property.to_string())
                    {
                        return Err(ProcessorError::Processing(format!(
                            "Base class {} has a property defined, {}, for referencing pivot class, {}. Manifest specifies a different property, {}",
                            base_csv_class,
                            base_class_ref_property.label.as_ref().unwrap(),
                            pivot_class,
                            pivot_column_ref_property
                        )));
                    }
                }
            }

            tracing::debug!(
                "Validating pivot column class '{}' for base class '{}'",
                pivot_class,
                base_csv_class
            );

            if let Some(klass) = vocab.classes.values().find(|c| c.id == pivot_class_iri) {
                if let Some(range_props) = &klass.range {
                    let defined_class_props: Vec<&String> = vocab
                        .properties
                        .values()
                        .filter_map(|prop| {
                            let final_prop_iri = prop.id.final_iri();
                            if range_props
                                .iter()
                                .any(|r| 
                                    // matches!(r, PropertyDatatype::URI(Some(final_prop_iri)))
                                    match r {
                                    PropertyDatatype::URI(Some(iri)) => final_prop_iri == *iri,
                                    _ => false,
                                }
                                )
                            {
                                prop.label.as_ref()
                            } else {
                                None
                            }
                        })
                        .collect();

                    // Find any/all properties in pivot_column.columns that are not in defined_class_props
                    // If any are found, return an error
                    let invalid_columns: Vec<&String> = pivot_column
                        .columns
                        .iter()
                        .filter(|column| !defined_class_props.contains(column))
                        .collect();
                    if !invalid_columns.is_empty() {
                        return Err(ProcessorError::Processing(format!(
                            "Pivot column class '{}' has columns ({:?}) that are not properties of the class: {:?}",
                            pivot_class, invalid_columns, defined_class_props
                        )));
                    }
                } else {
                    return Err(ProcessorError::Processing(format!(
                        "Pivot column class '{}' has no properties defined to use for instance import",
                        pivot_class
                    )));
                }
            } else {
                return Err(ProcessorError::Processing(format!(
                    "Pivot column class '{}' not found in vocabulary",
                    pivot_class
                )));
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

        // // Get the vocabulary map
        // let vocab = self.vocabulary.as_ref().ok_or_else(|| {
        //     ProcessorError::Processing("Vocabulary must be set before processing instances".into())
        // })?;

        tracing::debug!("Getting identifier label for class '{}'", parent_class_type);

        // Get the identifier property for this class
        let identifier_label = self
            .vocabulary
            .as_ref()
            .unwrap()
            .get_identifier_label(&parent_class_type)
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
                    "Identifier column '{}' not found in headers",
                    identifier_label
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
        for result in rdr.records() {
            let record = result.map_err(|e| {
                ProcessorError::Processing(format!("Failed to read CSV record: {}", e))
            })?;

            // Get the identifier value
            let id = record
                .get(id_column_index)
                .ok_or_else(|| ProcessorError::Processing("Missing identifier value".into()))?;

            // Get the subclass reference
            let subclass_ref = record
                .get(subclass_column_index)
                .ok_or_else(|| ProcessorError::Processing("Missing subclass reference".into()))?;

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
                    tracing::warn!(
                        "Subclass reference '{}' not found in vocabulary",
                        subclass_ref
                    );
                    IdOpt::String(subclass_ref.to_string())
                })
                .clone();

            let subclass_ref = subclass_ref.normalize().to_pascal_case();

            tracing::debug!(
                "[process_subclass_instance] Creating instance with parent class '{}' and subclass '{:?}'",
                parent_class_type,
                subclass_ref
            );

            let mut properties = serde_json::Map::new();

            // Map CSV columns to JSON-LD properties (excluding id and subclass columns)
            for (i, header) in headers.iter().enumerate() {
                if i != id_column_index && i != subclass_column_index {
                    if let Some(value) = record.get(i) {
                        // Skip empty values
                        if !value.is_empty() {
                            // Handle numeric values
                            if value.contains('$') || value.contains('%') {
                                let cleaned_value = value.replace(['$', '%', ','], "");
                                if let Ok(num) = cleaned_value.parse::<f64>() {
                                    properties.insert(
                                        header.to_string(),
                                        serde_json::Value::Number(
                                            serde_json::Number::from_f64(num).unwrap(),
                                        ),
                                    );
                                    continue;
                                }
                            }
                            // Default to string values
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
                id: id.to_string(),
                type_: vec![IdOpt::String(parent_class_type.clone()), subclass_ref],
                properties,
            };

            self.update_or_insert_instance(instance)?;
        }

        Ok(())
    }

    pub async fn process_properties_instance(
        &mut self,
        step: &ImportStep,
        instance_path: &str,
    ) -> Result<(), ProcessorError> {
        let class_type = step.instance_type.clone();

        // Get the vocabulary map
        let vocab = self.vocabulary.as_ref().ok_or_else(|| {
            ProcessorError::Processing("Vocabulary must be set before processing instances".into())
        })?;

        // Get the identifier property for this class
        let identifier_label = vocab.get_identifier_label(&class_type).ok_or_else(|| {
            ProcessorError::Processing(format!(
                "No identifier property found for class '{}'. To import instances of this class, your data model CSVs must indicate which column of the instance data will be used as the identifier (\"@id\") for instances of this class.",
                class_type
            ))
        })?;

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

        // Find column indices
        let entity_id_index = headers
            .iter()
            .position(|h| h == identifier_label)
            .ok_or_else(|| {
                ProcessorError::Processing(format!(
                    "Identifier column '{}' not found in headers",
                    identifier_label
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
        for result in rdr.records() {
            let record = result.map_err(|e| {
                ProcessorError::Processing(format!("Failed to read CSV record: {}", e))
            })?;

            let entity_id = record
                .get(entity_id_index)
                .ok_or_else(|| ProcessorError::Processing("Missing identifier value".into()))?;

            let property_id = record
                .get(property_id_index)
                .ok_or_else(|| ProcessorError::Processing("Missing Property ID".into()))?;

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
                    tracing::warn!("Property ID '{}' not found in vocabulary", property_id);
                    IdOpt::String(property_id.to_string())
                });

            let property_value = record
                .get(property_value_index)
                .ok_or_else(|| ProcessorError::Processing("Missing Property Value".into()))?;

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
                    let mut properties = serde_json::Map::new();
                    properties.insert(
                        property_id.final_iri(),
                        serde_json::Value::String(property_value.to_string()),
                    );
                    JsonLdInstance {
                        id: entity_id.to_string(),
                        type_: vec![IdOpt::String(class_type.clone())],
                        properties,
                    }
                });
        }

        Ok(())
    }

    fn update_or_insert_instance(
        &mut self,
        instance: JsonLdInstance,
    ) -> Result<(), ProcessorError> {
        let id = instance.id.clone();

        match self.instances.entry(id) {
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
