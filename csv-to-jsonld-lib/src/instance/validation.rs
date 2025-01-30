use super::types::InstanceProcessor;
use crate::error::ProcessorError;
use crate::types::{Header, IdOpt, PivotColumn, PropertyDatatype};
use crate::utils::{expand_iri_with_base, to_pascal_case};
use std::collections::HashSet;

impl InstanceProcessor {
    pub(crate) fn validate_headers(
        &mut self,
        headers: &[String],
        class_type: &str,
        identifier_label: &str,
        pivot_columns: Option<&Vec<PivotColumn>>,
        map_to_label: Option<&String>,
    ) -> Result<Vec<Option<Header>>, ProcessorError> {
        let valid_labels =
            self.get_valid_property_labels(class_type, pivot_columns, map_to_label)?;

        let mut unknown_headers = Vec::new();
        let ignorable_headers = match self.ignore.get(class_type) {
            Some(headers) => headers.as_slice(),
            None => &[],
        };

        let mut final_headers = Vec::new();

        for header in headers {
            // Skip if it's an empty string, unless on strict mode
            if header.is_empty() {
                self.processing_state.add_warning(
                    format!("Empty column found in CSV for class: {}", class_type),
                    Some("header_validation".to_string()),
                );
                final_headers.push(None);
                continue;
            }

            // Skip if it's the identifier column
            if header == identifier_label {
                let final_header = Header {
                    name: header.clone(),
                    datatype: PropertyDatatype::ID,
                    is_label_header: false,
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
                self.processing_state
                    .add_warning(message, Some("header_validation".to_string()));
            }
        }

        Ok(final_headers)
    }

    pub(crate) fn get_valid_property_labels(
        &mut self,
        class_type: &str,
        pivot_columns: Option<&Vec<PivotColumn>>,
        map_to_label: Option<&String>,
    ) -> Result<HashSet<Header>, ProcessorError> {
        let mut valid_labels: HashSet<Option<Header>> = HashSet::new();
        let vocab = self.vocabulary.as_mut().unwrap();
        let class_iri = expand_iri_with_base(&self.model_base_iri, class_type);

        if let Some(pivot_columns) = pivot_columns {
            for pivot_column in pivot_columns {
                let pivot_class = pivot_column.instance_type.to_string();
                let pivot_class_iri =
                    IdOpt::String(pivot_class.clone()).with_base_iri(self.model_base_iri.as_str());
                if let Some(class) = vocab.classes.values().find(|c| c.id == pivot_class_iri) {
                    tracing::debug!("[get_valid_property_labels] Matched pivot class: {:?} with class iri: {:?}", pivot_class_iri, class);
                    if let Some(props) = &class.range {
                        for prop_iri in props {
                            if let Some(prop) = vocab.properties.values().find(|p| match prop_iri {
                                PropertyDatatype::URI(Some(iri)) => p.id.final_iri() == *iri,
                                _ => false,
                            }) {
                                valid_labels.insert(Header::try_from(prop).ok());
                            }
                        }
                    }
                }
            }
        }

        if let Some(class) = vocab
            .classes
            .values_mut()
            .find(|c| c.id.final_iri() == class_iri)
        {
            if let Some(props) = &mut class.range {
                for prop_iri in props {
                    if let Some(prop) = vocab.properties.values().find(|p| match prop_iri {
                        PropertyDatatype::URI(Some(iri))
                        | PropertyDatatype::Picklist(Some(iri)) => p.id.final_iri() == *iri,
                        _ => false,
                    }) {
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

        let mut valid_labels: HashSet<Header> = valid_labels.into_iter().flatten().collect();

        if let Some(map_to_label) = map_to_label {
            let mut map_to_label_header = valid_labels.take(&Header {
                name: map_to_label.clone(),
                ..Default::default()
            });
            match map_to_label_header.as_mut() {
                Some(header) => {
                    header.set_is_label_header(true);
                    valid_labels.insert(header.clone());
                }
                None => {
                    let message = format!(
                        "Column for mapToLabel ({}) is either not of type, String, or is not found in valid labels for class '{}'. Expected one of: {}",
                        map_to_label,
                        class_type,
                        valid_labels.iter().map(|h| h.name.clone()).collect::<Vec<_>>().join(", ")
                    );
                    if self.is_strict {
                        return Err(ProcessorError::InvalidManifest(message));
                    } else {
                        self.processing_state
                            .add_warning(message, Some("property_validation".to_string()));
                    }
                }
            }
        }

        Ok(valid_labels)
    }

    pub fn validate_pivot_columns(
        &self,
        pivot_columns: Vec<&PivotColumn>,
        base_csv_class: &str,
    ) -> Result<(), ProcessorError> {
        let vocab = self.vocabulary.as_ref().unwrap();

        for pivot_column in pivot_columns {
            let pivot_class = pivot_column.instance_type.to_string();
            let pivot_class_iri =
                IdOpt::String(pivot_class.clone()).with_base_iri(self.model_base_iri.as_str());

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

            if let Some(klass) = vocab.classes.values().find(|c| c.id == pivot_class_iri) {
                if let Some(range_props) = &klass.range {
                    let defined_class_props: Vec<&String> = vocab
                        .properties
                        .values()
                        .filter_map(|prop| {
                            let final_prop_iri = prop.id.final_iri();
                            if range_props.iter().any(|r| match r {
                                PropertyDatatype::URI(Some(iri)) => final_prop_iri == *iri,
                                _ => false,
                            }) {
                                prop.label.as_ref()
                            } else {
                                None
                            }
                        })
                        .collect();

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
}
