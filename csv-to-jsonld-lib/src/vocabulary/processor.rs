use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use super::mapping::{MappingConfig, RowValues, VocabularyColumnMapping};
use crate::error::ProcessorError;
use crate::manifest::{ImportStep, ModelStep, StepType};
use crate::types::{IdOpt, OnEntity, PropertyDatatype, VocabularyMap, VocabularyTerm};
use crate::utils::{expand_iri_with_base, map_xsd_type, to_pascal_case};
use crate::{contains_variant, Manifest};

pub struct VocabularyProcessor {
    manifest: Arc<Manifest>,
    pub vocabulary: VocabularyMap,
    class_properties: HashMap<IdOpt, Vec<String>>,
    is_strict: bool,
    ignore: HashMap<String, Vec<String>>,
}

impl VocabularyProcessor {
    pub fn new(manifest: Arc<Manifest>, is_strict: bool) -> Self {
        let ignore = manifest
            .model
            .sequence
            .iter()
            .fold(HashMap::new(), |mut acc, step| {
                if let Some(ignores) = &step.ignore {
                    acc.insert(step.path.clone(), ignores.clone());
                }
                acc
            });
        Self {
            manifest,
            vocabulary: VocabularyMap::new(),
            class_properties: HashMap::new(),
            is_strict,
            ignore,
        }
    }

    pub fn new_from_vocab_meta(
        manifest: Arc<Manifest>,
        path: &PathBuf,
        is_strict: bool,
    ) -> Result<Self, ProcessorError> {
        let vocabulary = VocabularyMap::from_file(path)?;
        Ok(Self {
            manifest,
            vocabulary,
            class_properties: HashMap::new(),
            is_strict,
            ignore: HashMap::new(),
        })
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
        } else if contains_variant!(
            &step.types,
            StepType::ModelStep(ModelStep::PropertiesVocabularyStep)
        ) {
            MappingConfig {
                type_: StepType::ModelStep(ModelStep::PropertiesVocabularyStep),
                column_mapping: VocabularyColumnMapping::property_vocabulary_step(),
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
        mapping
            .column_mapping
            .validate_headers(headers, is_strict)?;

        Ok(mapping)
    }

    pub async fn process_vocabulary(
        &mut self,
        step: ImportStep,
        model_path: &str,
    ) -> Result<(), ProcessorError> {
        let step_path = &step.path.clone();
        let file_path = PathBuf::from(model_path).join(step_path);
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
        let headers = rdr.headers().map_err(|e| {
            ProcessorError::Processing(format!("Failed to read CSV headers: {}", e))
        })?;

        tracing::debug!("CSV headers: {:?}", headers);

        let sub_class_of = step.sub_class_of.clone();

        let mapping = Self::from_headers(headers, step, self.is_strict)?;

        let ignorable_headers = self.ignore.get(step_path);

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
        for result in rdr.records() {
            let record = result.map_err(|e| {
                ProcessorError::Processing(format!("Failed to read CSV record: {}", e))
            })?;

            let row_values = mapping.extract_values(&record, &headers)?;

            // tracing::debug!("Row values: {:#?}", row_values);

            self.process_class_term(&row_values, sub_class_of.clone())?;
            if !matches!(
                &mapping.type_,
                StepType::ModelStep(ModelStep::SubClassVocabularyStep)
            ) {
                self.process_property_term(&row_values)?;
            }
        }

        let picklist_classes = self
            .vocabulary
            .properties
            .values()
            .filter_map(|p| {
                if let Some(PropertyDatatype::Picklist(class)) = p.range.as_ref()?.first() {
                    class.as_ref().map(|string| {
                        IdOpt::String(string.clone())
                            .without_base_iri(&self.manifest.model.base_iri)
                    })
                    // class.as_ref().map(|string| IdOpt::String(string.clone()))
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
            self.handle_add_rdfs_label_property(&class_id)?;
        }

        // Update class terms with their properties
        for (class_id, properties) in &self.class_properties {
            if let Some(class_term) = self.vocabulary.classes.get_mut(class_id) {
                class_term.range = Some(
                    properties
                        .iter()
                        .map(|p| {
                            // PropertyDatatype::URI(Some(format!(
                            //     "{}{}",
                            //     self.manifest.model.base_iri, p
                            // )))
                            PropertyDatatype::URI(Some(expand_iri_with_base(
                                &self.manifest.model.base_iri,
                                p,
                            )))
                        })
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
        for extra_item in extra_items.values() {
            if matches!(extra_item.on_entity, OnEntity::Class) {
                extra_items_result.insert(
                    extra_item.map_to.clone(),
                    extra_item.value.clone().unwrap_or("".to_string()),
                );
            }
        }

        // let label = if class_name.is_empty() {
        //     tracing::debug!("Class name is empty, using class_id");
        //     match class_id {
        //         IdOpt::String(class_id) => class_id.to_string(),
        //         IdOpt::ReplacementMap { replacement_id, .. } => replacement_id.to_string(),
        //     }
        // } else {
        //     class_name.to_string()
        // };

        match self.vocabulary.classes.entry(class_id.clone()) {
            Entry::Vacant(_) => {
                let class_term = VocabularyTerm {
                    id: class_id
                        .normalize()
                        .to_pascal_case()
                        .with_base_iri(&self.manifest.model.base_iri),
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
                        .with_base_iri(&self.manifest.model.base_iri),
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

        // if let std::collections::hash_map::Entry::Vacant(_) =
        //     self.vocabulary.classes.entry(class_id.clone())
        // {
        //     let class_term = VocabularyTerm {
        //         id: class_id
        //             .to_pascal_case()
        //             .with_base_iri(&self.manifest.model.base_iri),
        //         type_: vec!["rdfs:Class".to_string()],
        //         sub_class_of,
        //         label,
        //         comment: Some(class_description.to_string()),
        //         domain: None,
        //         range: Some(vec![]),
        //         extra_items: extra_items_result,
        //     };

        //     tracing::debug!("Adding class term: {:#?}", class_term);

        //     self.vocabulary.classes.insert(class_id.clone(), class_term);
        // }
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
            // let value = format!(
            //     "{}{}",
            //     self.manifest.model.base_iri,
            //     to_pascal_case(property_class)
            // );
            let value = match xsd_type {
                // PropertyDatatype::Picklist(_) => PropertyDatatype::Picklist(Some(format!(
                //     "{}{}",
                //     self.manifest.model.base_iri,
                //     to_pascal_case(property_class)
                // ))),
                PropertyDatatype::Picklist(_) => {
                    PropertyDatatype::Picklist(Some(expand_iri_with_base(
                        &self.manifest.model.base_iri,
                        &to_pascal_case(property_class),
                    )))
                }
                PropertyDatatype::URI(_) | PropertyDatatype::ID => {
                    // PropertyDatatype::URI(Some(format!(
                    //     "{}{}",
                    //     self.manifest.model.base_iri,
                    //     to_pascal_case(property_class)
                    // )))
                    PropertyDatatype::URI(Some(expand_iri_with_base(
                        &self.manifest.model.base_iri,
                        &to_pascal_case(property_class),
                    )))
                }
                _ => {
                    let error_string = format!(
                        "[Property: {}] A property with type {} cannot have a target class ({})",
                        property_name, property_type, property_class
                    );
                    if self.is_strict {
                        return Err(ProcessorError::Processing(error_string));
                    } else {
                        tracing::warn!("{}", error_string);
                        // PropertyDatatype::URI(Some(format!(
                        //     "{}{}",
                        //     self.manifest.model.base_iri,
                        //     to_pascal_case(property_class)
                        // )))
                        PropertyDatatype::URI(Some(expand_iri_with_base(
                            &self.manifest.model.base_iri,
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
            id: camel_name.with_base_iri(&self.manifest.model.base_iri),
            type_: vec!["rdf:Property".to_string()],
            sub_class_of: None,
            label: Some(property_name.to_string()),
            comment: Some(property_desc.to_string()),
            domain: Some(vec![new_or_existing_class_id
                .normalize()
                .to_pascal_case()
                .with_base_iri(&self.manifest.model.base_iri)
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

        // if matches!(xsd_type, PropertyDatatype::Picklist(_)) {
        //     self.handle_add_rdfs_label_property(property_class)?;
        // }
        Ok(())
    }

    fn handle_add_rdfs_label_property(&mut self, class_id: &IdOpt) -> Result<(), ProcessorError> {
        // class_id is the identifier of the class that the picklist property is pointing to
        // we need to do two things:
        // 1. Find that class in the vocabulary and add the rdfs:label property to its range
        // 2. Add the rdfs:label property to the vocabulary properties map. If it exists, update it with the new class as its domain. If it doesn't exist, create it with the new class as its domain.
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
            return Err(ProcessorError::Processing(format!(
                "Picklist Class {} not found in vocabulary while adding label",
                class_id
            )));
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
                    // range: Some(vec![PropertyDatatype::URI(Some("xsd:string".to_string()))]),
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
                    // range: Some(vec![PropertyDatatype::URI(Some("xsd:string".to_string()))]),
                    range: Some(vec![PropertyDatatype::String]),
                    extra_items: HashMap::new(),
                    one_of: None,
                })?;
            }
        }

        Ok(())
    }

    pub fn take_vocabulary(&mut self) -> VocabularyMap {
        std::mem::take(&mut self.vocabulary)
    }
}
