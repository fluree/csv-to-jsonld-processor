use serde::Serialize;

use crate::error::ProcessorError;
use crate::manifest::ImportStep;
use crate::types::{
    ColumnOverride, ExtraItem, FlureeDataModel, JsonLdVocabulary, VocabularyMap, VocabularyTerm,
};
use crate::Manifest;
use std::collections::hash_map::Entry;
use std::path::Path;
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};
use std::{fs, mem};

#[derive(Debug, Serialize)]
struct VocabularyColumnMapping {
    class_column: String,
    class_description_column: String,
    property_column: String,
    property_description_column: String,
    type_column: String,
    property_class_column: String,
    extra_items: HashMap<String, String>,
}

impl VocabularyColumnMapping {
    fn from_headers(
        headers: &csv::StringRecord,
        overrides: &[ColumnOverride],
        extra_items: &[ExtraItem],
        is_strict: bool,
    ) -> Result<Self, ProcessorError> {
        // Default column names
        let mut mapping = Self {
            class_column: "Class".to_string(),
            class_description_column: "Class Description".to_string(),
            property_column: "Property Name".to_string(),
            property_description_column: "Property Description".to_string(),
            type_column: "Type".to_string(),
            property_class_column: "Class Range".to_string(), // The second "Class" column
            extra_items: HashMap::new(),
        };

        // Apply any overrides from the manifest
        for override_ in overrides {
            mapping.handle_override(override_)?;
        }

        for extra_item in extra_items {
            mapping
                .extra_items
                .insert(extra_item.column.clone(), extra_item.map_to.clone());
        }

        // Verify all required columns exist
        let required_columns = [
            (&mapping.class_column, "Class"),
            (&mapping.property_column, "Property Name"),
            (&mapping.type_column, "Type"),
        ];

        for (column, name) in required_columns.iter() {
            if !headers.iter().any(|h| h == column.as_str()) {
                return Err(ProcessorError::Processing(format!(
                    "Required column '{}' not found in CSV headers",
                    name
                )));
            }
        }

        let mut json_value_of_mapping = serde_json::to_value(&mapping).unwrap();
        let json_object_of_mapping = json_value_of_mapping.as_object_mut().unwrap();
        for (extra_item, map_to) in mapping.extra_items.iter() {
            json_object_of_mapping.insert(
                map_to.to_string(),
                serde_json::Value::String(extra_item.clone()),
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

    fn get_value<'a>(
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

    pub fn handle_override(&mut self, override_: &ColumnOverride) -> Result<(), ProcessorError> {
        match override_.map_to.as_str() {
            "Class.ID" => self.class_column = override_.column.clone(),
            "Class.Description" => self.class_description_column = override_.column.clone(),
            "Property.ID" => self.property_column = override_.column.clone(),
            "Property.Description" => self.property_description_column = override_.column.clone(),
            "Property.Type" => self.type_column = override_.column.clone(),
            "Property.TargetClass" => self.property_class_column = override_.column.clone(),
            _ => {
                return Err(ProcessorError::Processing(format!(
                    "Invalid override mapTo value: {}. Overrides must be one of the following: Class.ID, Class.Description, Property.ID, Property.Description, Property.Type, Property.TargetClass. If you want to specify an extraItem, use the extraItems field in the manifest",
                    override_.map_to
                )));
            }
        }
        Ok(())
    }
}

fn to_pascal_case(s: &str) -> String {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| {
            let mut c = s.chars();
            match c.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().chain(c).collect(),
            }
        })
        .collect()
}

fn to_camel_case(s: &str) -> String {
    // Special handling for B/(W) -> BW
    let s = s.replace("B/(W)", "BW");

    let pascal = to_pascal_case(&s);
    let mut c = pascal.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_lowercase().chain(c).collect(),
    }
}

fn map_xsd_type(csv_type: &str) -> String {
    match csv_type.to_lowercase().as_str() {
        "@id" => "ID".to_string(),
        "uri" => "anyURI".to_string(),
        "identifier" => "ID".to_string(),
        "string" => "string".to_string(),
        "float" => "decimal".to_string(),
        "integer" => "integer".to_string(),
        "date" => "date".to_string(),
        "calculation" => "decimal".to_string(),
        _ => "string".to_string(),
    }
}

pub struct VocabularyProcessor {
    manifest: Arc<Manifest>,
    vocabulary: VocabularyMap,
    class_properties: HashMap<String, Vec<String>>,
    is_strict: bool,
}

impl VocabularyProcessor {
    // Previous methods remain the same until process_property_term...
    pub fn new(manifest: Arc<Manifest>, is_strict: bool) -> Self {
        Self {
            manifest,
            vocabulary: VocabularyMap::new(),
            class_properties: HashMap::new(),
            is_strict,
        }
    }

    // Add method to get vocabulary
    pub fn get_vocabulary(&self) -> VocabularyMap {
        VocabularyMap {
            classes: self.vocabulary.classes.clone(),
            properties: self.vocabulary.properties.clone(),
            identifiers: self.vocabulary.identifiers.clone(),
        }
    }

    pub async fn process_base_vocabulary(
        &mut self,
        step: &ImportStep,
        model_path: &str,
    ) -> Result<(), ProcessorError> {
        let file_path = PathBuf::from(model_path).join(&step.path);
        tracing::debug!("Reading vocabulary data from {:?}", file_path);

        let mut rdr: csv::Reader<std::fs::File> = csv::Reader::from_path(&file_path)
            .map_err(|e| ProcessorError::Processing(format!("Failed to read CSV: {}", e)))?;

        // Get headers and build column mapping
        let headers = rdr
            .headers()
            .map_err(|e| ProcessorError::Processing(format!("Failed to read CSV headers: {}", e)))?
            .clone();

        tracing::debug!("CSV headers: {:?}", headers);

        let mapping = VocabularyColumnMapping::from_headers(
            &headers,
            &step.overrides,
            &step.extra_items,
            self.is_strict,
        )?;

        tracing::debug!("Vocabulary column mapping: {:?}", mapping);

        // Process each row
        for result in rdr.records() {
            let record = result.map_err(|e| {
                ProcessorError::Processing(format!("Failed to read CSV record: {}", e))
            })?;

            // Get values using the mapping
            let class_name = mapping
                .get_value(&record, &headers, &mapping.class_column)
                .ok_or_else(|| ProcessorError::Processing("Missing Class column".into()))?;
            let class_desc = mapping
                .get_value(&record, &headers, &mapping.class_description_column)
                .ok_or_else(|| {
                    ProcessorError::Processing("Missing Class Description column".into())
                })?;
            let property = mapping
                .get_value(&record, &headers, &mapping.property_column)
                .ok_or_else(|| ProcessorError::Processing("Missing Property column".into()))?;
            let property_desc = mapping
                .get_value(&record, &headers, &mapping.property_description_column)
                .ok_or_else(|| {
                    ProcessorError::Processing("Missing Property Description column".into())
                })?;
            let property_type = mapping
                .get_value(&record, &headers, &mapping.type_column)
                .ok_or_else(|| ProcessorError::Processing("Missing Type column".into()))?;
            let property_class = mapping
                .get_value(&record, &headers, &mapping.property_class_column)
                .unwrap_or("");
            let extra_items = mapping
                .extra_items
                .iter()
                .map(|(expected_csv_header, concept_to_extract)| {
                    (
                        concept_to_extract.clone(),
                        mapping
                            .get_value(&record, &headers, expected_csv_header)
                            .unwrap_or("")
                            .to_string(),
                    )
                })
                .collect::<HashMap<_, _>>();

            self.process_class_term(class_name, class_desc)?;
            self.process_property_term(
                property,
                property_desc,
                property_type,
                property_class,
                class_name,
                extra_items,
            )?;
        }

        // Update class terms with their properties
        for (class_name, properties) in &self.class_properties {
            if let Some(class_term) = self.vocabulary.classes.get_mut(class_name) {
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
        class_name: &str,
        class_desc: &str,
    ) -> Result<(), ProcessorError> {
        if !class_name.is_empty() {
            let pascal_name = to_pascal_case(class_name);
            if let std::collections::hash_map::Entry::Vacant(_) =
                self.vocabulary.classes.entry(pascal_name.clone())
            {
                let class_term = VocabularyTerm {
                    id: format!("{}{}", self.manifest.model.base_iri, pascal_name),
                    type_: vec!["rdfs:Class".to_string()],
                    label: class_name.to_string(),
                    comment: Some(class_desc.to_string()),
                    domain: None,
                    range: Some(vec![]),
                    extra_items: HashMap::new(),
                };
                self.vocabulary.classes.insert(pascal_name, class_term);
            }
        }
        Ok(())
    }

    fn process_property_term(
        &mut self,
        property: &str,
        property_desc: &str,
        property_type: &str,
        property_class: &str,
        class_name: &str,
        extra_items: HashMap<String, String>,
    ) -> Result<(), ProcessorError> {
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

            // Create property term
            let property_term = VocabularyTerm {
                id: format!("{}{}", self.manifest.model.base_iri, camel_name),
                type_: vec!["rdf:Property".to_string()],
                label: property.to_string(),
                comment: Some(property_desc.to_string()),
                domain: Some(vec![format!(
                    "{}{}",
                    self.manifest.model.base_iri,
                    to_pascal_case(class_name)
                )]),
                range,
                extra_items,
            };

            // If it's an ID property, store it in identifiers map
            if xsd_type == "ID" {
                self.vocabulary
                    .identifiers
                    .insert(to_pascal_case(class_name), property_term);
            } else {
                // Otherwise store it in properties map
                match self.vocabulary.properties.entry(camel_name.clone()) {
                    Entry::Vacant(_) => {
                        self.vocabulary.properties.insert(camel_name, property_term);
                    }
                    Entry::Occupied(mut entry) => entry.get_mut().update_with(property_term)?,
                }

                // Track properties for each class
                if !class_name.is_empty() {
                    let class_entry = self
                        .class_properties
                        .entry(to_pascal_case(class_name))
                        .or_default();

                    if !property.is_empty() {
                        class_entry.push(to_camel_case(property));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn save_vocabulary(&mut self, output_path: &PathBuf) -> Result<(), ProcessorError> {
        let ledger = self.manifest.id.clone();
        let label = self.manifest.name.clone();
        let comment = self.manifest.description.clone();
        let insert = FlureeDataModel {
            type_: vec!["f:DataModel".to_string()],
            label,
            comment,
            // Only include regular properties in output, not identifiers
            classes: mem::take(&mut self.vocabulary.classes)
                .into_values()
                .collect(),
            properties: mem::take(&mut self.vocabulary.properties)
                .into_values()
                .collect(),
        };
        let vocabulary = JsonLdVocabulary {
            context: serde_json::json!({
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "f": "https://ns.flur.ee/ledger#",
                "rdfs:domain": { "@type": "@id" },
                "rdfs:range": { "@type": "@id" }
            }),
            ledger,
            insert,
        };

        let vocab_json = serde_json::to_string_pretty(&vocabulary).map_err(|e| {
            ProcessorError::Processing(format!("Failed to serialize vocabulary: {}", e))
        })?;

        let output_dir = if output_path.is_dir() {
            output_path.clone()
        } else {
            // If the path doesn't exist yet, assume it's intended to be a directory
            Path::new(&output_path).to_path_buf()
        };

        // Create the full file path: output_dir/vocabulary.jsonld
        let output_path = output_dir.join("vocabulary.jsonld");

        // Ensure the directory exists
        fs::create_dir_all(&output_dir).map_err(|e| {
            ProcessorError::Processing(format!(
                "Failed to create directory for vocabulary file: {}",
                e
            ))
        })?;

        // Write the JSON to the file
        fs::write(&output_path, vocab_json).map_err(|e| {
            ProcessorError::Processing(format!("Failed to write vocabulary file: {}", e))
        })?;

        let output_path_str = output_path.to_string_lossy();
        tracing::info!("Saved vocabulary to {}", output_path_str);

        Ok(())
    }
}
