use super::types::InstanceProcessor;
use crate::error::ProcessorError;
use crate::types::{IdOpt, PropertyDatatype};
use crate::utils::{expand_iri_with_base, to_kebab_case, DATE_FORMATS};
use serde_json::Value as JsonValue;

impl InstanceProcessor {
    pub(crate) fn process_value(
        &mut self,
        value: &str,
        datatype: &PropertyDatatype,
        header_name: &str,
        row_num: usize,
    ) -> Result<JsonValue, ProcessorError> {
        match datatype {
            PropertyDatatype::ID => Ok(JsonValue::String(value.to_string())),
            PropertyDatatype::Date => {
                let trimmed_value = value.trim();
                tracing::info!("Trimmed date value: {}", trimmed_value);
                let date_result = DATE_FORMATS
                    .iter()
                    .find_map(|fmt| {
                        // First try exact parsing
                        if let Ok(date) = chrono::NaiveDate::parse_from_str(trimmed_value, fmt) {
                            return Some(date);
                        }

                        // Handle partial dates
                        match *fmt {
                            // Year only - default to Jan 1
                            "%Y" => {
                                trimmed_value.parse::<i32>().ok().and_then(|year| {
                                    chrono::NaiveDate::from_ymd_opt(
                                        year, 1, // January
                                        1, // 1st
                                    )
                                })
                            }
                            // Year-month formats - default to 1st of month
                            "%Y-%m" | "%Y/%m" | "%b %Y" | "%B %Y" | "%m-%Y" => {
                                if let Ok(parsed) = chrono::NaiveDate::parse_from_str(
                                    &format!("{}-01", trimmed_value.replace("/", "-")),
                                    "%Y-%m-%d",
                                ) {
                                    Some(parsed)
                                } else if let Ok(parsed) = chrono::NaiveDate::parse_from_str(
                                    &format!("01 {}", trimmed_value),
                                    "%d %B %Y",
                                ) {
                                    Some(parsed)
                                } else if let Ok(parsed) = chrono::NaiveDate::parse_from_str(
                                    &format!("01 {}", trimmed_value),
                                    "%d %b %Y",
                                ) {
                                    Some(parsed)
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        }
                    })
                    .ok_or_else(|| {
                        ProcessorError::Processing(format!("Failed to parse date {:#?}", value))
                    });

                if let Ok(date) = date_result {
                    Ok(JsonValue::String(date.format("%Y-%m-%d").to_string()))
                } else {
                    let msg = format!("Failed to parse date {:#?}", value);
                    if self.is_strict {
                        Err(ProcessorError::Processing(msg))
                    } else {
                        self.processing_state.add_warning(
                            format!("{}, using raw value", msg),
                            Some("date_validation".to_string()),
                        );
                        Ok(JsonValue::String(value.to_string()))
                    }
                }
            }
            PropertyDatatype::Integer => {
                let cleaned_value = value.replace(['$', '%', ','], "");
                if let Ok(num) = cleaned_value.parse::<i64>() {
                    Ok(JsonValue::Number(serde_json::Number::from(num)))
                } else if self.is_strict {
                    Err(ProcessorError::Processing(format!(
                        "[Column: {}, Row: {}], Invalid integer value: {}",
                        header_name,
                        row_num + 1,
                        value
                    )))
                } else {
                    self.processing_state.add_warning(
                        format!(
                            "[Column: {}, Row: {}], Invalid integer value: {}. Serializing as string.",
                            header_name,
                            row_num + 1,
                            value
                        ),
                        Some("value_validation".to_string()),
                    );
                    Ok(JsonValue::String(value.to_string()))
                }
            }
            PropertyDatatype::Decimal => {
                let cleaned_value = value.replace(['$', '%', ','], "");
                if let Ok(num) = cleaned_value.parse::<f64>() {
                    Ok(JsonValue::Number(
                        serde_json::Number::from_f64(num).unwrap(),
                    ))
                } else {
                    Ok(JsonValue::String(cleaned_value.to_string()))
                }
            }
            PropertyDatatype::String => Ok(JsonValue::String(value.to_string())),
            PropertyDatatype::Boolean => {
                let cleaned_value = value.to_lowercase();
                if cleaned_value == "true"
                    || cleaned_value == "false"
                    || cleaned_value == "1"
                    || cleaned_value == "0"
                    || cleaned_value == "yes"
                    || cleaned_value == "no"
                {
                    Ok(JsonValue::Bool(
                        cleaned_value == "true" || cleaned_value == "1" || cleaned_value == "yes",
                    ))
                } else {
                    let msg = format!(
                        "[Column: {}, Row: {}], Invalid boolean value: {}",
                        header_name,
                        row_num + 1,
                        value
                    );
                    if self.is_strict {
                        Err(ProcessorError::Processing(msg))
                    } else {
                        self.processing_state.add_warning(
                            format!("{}, using raw value", msg),
                            Some("boolean_validation".to_string()),
                        );
                        Ok(JsonValue::String(value.to_string()))
                    }
                }
            }
            PropertyDatatype::URI(target_class) | PropertyDatatype::Picklist(target_class) => {
                self.process_class_restricted_value(header_name, value, target_class, datatype)
            }
        }
    }

    pub(crate) fn process_class_restricted_value(
        &mut self,
        header: &str,
        value: &str,
        target_class: &Option<String>,
        datatype: &PropertyDatatype,
    ) -> Result<JsonValue, ProcessorError> {
        let class_match = match target_class {
            Some(target_class) => {
                self.vocabulary
                    .as_ref()
                    .unwrap()
                    .classes
                    .iter()
                    .find(|(id, _)| {
                        let final_id = id
                            .normalize()
                            .to_pascal_case()
                            .with_base_iri(&self.model_base_iri);
                        match final_id {
                            IdOpt::String(string_id) => &string_id == target_class,
                            IdOpt::ReplacementMap { original_id, .. } => {
                                &original_id == target_class
                            }
                        }
                    })
            }
            None => None,
        };

        if matches!(datatype, PropertyDatatype::Picklist(_)) {
            let (class_match, class_match_class_definition) = class_match.ok_or_else(|| {
                ProcessorError::Processing(format!(
                    "Class match not found for picklist value: {}",
                    value
                ))
            })?;
            let enum_picklist = match class_match_class_definition.one_of.clone() {
                Some(one_of) => one_of,
                None => {
                    let error = ProcessorError::Processing(format!(
                    "Class match found ({}) for picklist value ({}) on header ({}), but no picklist enums defined on class.",
                    class_match, value, header
                ));
                    if self.is_strict {
                        return Err(error);
                    } else {
                        self.processing_state.add_warning(
                            error.to_string(),
                            Some("picklist_validation".to_string()),
                        );
                        vec![]
                    }
                }
            };
            let iri = expand_iri_with_base(
                &self.instances_base_iri,
                &format!(
                    "{}/{}",
                    to_kebab_case(class_match.to_string().as_ref()),
                    value
                ),
            );
            let does_picklist_contain_value = enum_picklist
                .iter()
                .any(|picklist_value| picklist_value.to_string() == iri);
            if !does_picklist_contain_value {
                let error_string = format!(
                    "Value \"{}\" ({}) for property \"{}\" not found in {} picklist: {:?}",
                    value, iri, header, class_match, enum_picklist
                );
                if self.is_strict {
                    return Err(ProcessorError::Processing(error_string));
                } else {
                    self.processing_state
                        .add_warning(error_string, Some("picklist_validation".to_string()));
                }
            }
        }

        let class_match_id = class_match.map(|(id, _)| id.clone());

        if let Some(class_id) = class_match_id {
            if self.is_namespace_iris {
                let iri = format!("{}/{}", to_kebab_case(class_id.to_string().as_ref()), value);
                Ok(JsonValue::String(iri))
            } else {
                Ok(JsonValue::String(value.to_string()))
            }
        } else {
            Ok(JsonValue::String(value.to_string()))
        }
    }
}
