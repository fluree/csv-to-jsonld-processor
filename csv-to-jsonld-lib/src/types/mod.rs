use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::ProcessorError;

fn are_conflicting<T>(value_one: &Option<T>, value_two: &Option<T>) -> bool
where
    T: PartialEq,
{
    match (value_one, value_two) {
        (Some(value_one), Some(value_two)) => value_one != value_two,
        (None, None) => false,
        _ => false,
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnOverride {
    pub column: String,
    #[serde(rename = "mapTo")]
    pub map_to: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExtraItem {
    pub column: String,
    #[serde(rename = "mapTo")]
    pub map_to: String,
}

#[derive(Debug, Clone)]
pub struct VocabularyTerm {
    pub id: String,
    pub type_: Vec<String>,
    pub label: String,
    pub comment: Option<String>,
    pub domain: Option<Vec<String>>,
    pub range: Option<Vec<String>>,
    pub extra_items: HashMap<String, String>,
}

impl VocabularyTerm {
    pub(crate) fn update_with(
        &mut self,
        other_entry: VocabularyTerm,
    ) -> Result<(), ProcessorError> {
        // if any of the following are true, we need to throw an error:
        // - the label is different
        // - the comment is different

        if self.label != other_entry.label {
            return Err(ProcessorError::Processing(format!(
                "The CSV uses conflicting labels for the same term '{}':\n\
                     - Label 1: {}\n\
                     - Label 2: {}",
                self.id, self.label, other_entry.label
            )));
        }

        if are_conflicting(&self.comment, &other_entry.comment) {
            let self_comment = &self.comment.as_ref().unwrap();
            let other_comment = &other_entry.comment.as_ref().unwrap();
            return Err(ProcessorError::Processing(format!(
                "The CSV uses conflicting comments for the same term '{}':\n\
                     - Comment 1: {:#?}\n\
                     - Comment 2: {:#?}",
                self.id, self_comment, other_comment
            )));
        }

        // if the domain or range are different, we need to merge them
        if let Some(domain) = &other_entry.domain {
            if let Some(self_domain) = &mut self.domain {
                for item in domain {
                    if !self_domain.contains(item) {
                        self_domain.push(item.clone());
                    }
                }
            } else {
                self.domain = Some(domain.clone());
            }
        }

        if let Some(range) = &other_entry.range {
            if let Some(self_range) = &mut self.range {
                for item in range {
                    if !self_range.contains(item) {
                        self_range.push(item.clone());
                    }
                }
            } else {
                self.range = Some(range.clone());
            }
        }

        for (key, value) in &other_entry.extra_items {
            if let Some(this_value) = self.extra_items.get(key) {
                if value != this_value {
                    return Err(ProcessorError::Processing(format!(
                        "The CSV uses conflicting values for the same term '{}':\n\
                             - Key: {}\n\
                             - Value 1: {}\n\
                             - Value 2: {}",
                        self.id, key, value, this_value
                    )));
                }
            }
            self.extra_items.insert(key.clone(), value.clone());
        }

        Ok(())
    }
}

impl Serialize for VocabularyTerm {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serde_json::Map::new();
        map.insert(
            "@id".to_string(),
            serde_json::Value::String(self.id.clone()),
        );
        map.insert(
            "@type".to_string(),
            serde_json::Value::Array(
                self.type_
                    .clone()
                    .into_iter()
                    .map(serde_json::Value::String)
                    .collect(),
            ),
        );
        map.insert(
            "rdfs:label".to_string(),
            serde_json::Value::String(self.label.clone()),
        );
        if let Some(comment) = &self.comment {
            map.insert(
                "rdfs:comment".to_string(),
                serde_json::Value::String(comment.clone()),
            );
        }
        if let Some(domain) = &self.domain {
            map.insert(
                "rdfs:domain".to_string(),
                serde_json::Value::Array(
                    domain
                        .clone()
                        .into_iter()
                        .map(serde_json::Value::String)
                        .collect(),
                ),
            );
        }
        if let Some(range) = &self.range {
            map.insert(
                "rdfs:range".to_string(),
                serde_json::Value::Array(
                    range
                        .clone()
                        .into_iter()
                        .map(serde_json::Value::String)
                        .collect(),
                ),
            );
        }
        for (key, value) in &self.extra_items {
            if !value.is_empty() {
                map.insert(key.clone(), serde_json::Value::String(value.clone()));
            }
        }
        map.serialize(serializer)
    }
}

#[derive(Debug, Serialize)]
pub struct FlureeDataModel {
    #[serde(rename = "rdfs:label")]
    pub label: String,
    #[serde(rename = "rdfs:comment")]
    pub comment: String,
    #[serde(rename = "@type")]
    pub type_: Vec<String>,
    pub properties: Vec<VocabularyTerm>,
    pub classes: Vec<VocabularyTerm>,
}

#[derive(Debug, Serialize)]
pub struct JsonLdVocabulary {
    #[serde(rename = "@context")]
    pub context: serde_json::Value,
    pub ledger: String,
    pub insert: FlureeDataModel,
}

#[derive(Debug, Serialize)]
pub struct JsonLdContext {
    #[serde(rename = "@context")]
    pub context: serde_json::Map<String, serde_json::Value>,
}

pub struct VocabularyMap {
    pub classes: HashMap<String, VocabularyTerm>,
    pub properties: HashMap<String, VocabularyTerm>,
    /// Maps class name to its identifier property term
    pub identifiers: HashMap<String, VocabularyTerm>,
}

impl VocabularyMap {
    pub fn new() -> Self {
        Self {
            classes: HashMap::new(),
            properties: HashMap::new(),
            identifiers: HashMap::new(),
        }
    }

    /// Get the identifier property label for a given class
    pub fn get_identifier_label(&self, class_name: &str) -> Option<&str> {
        self.identifiers
            .get(class_name)
            .map(|term| term.label.as_str())
    }
}
