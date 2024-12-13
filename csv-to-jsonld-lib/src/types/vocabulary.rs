use crate::utils::are_conflicting;
use crate::{error::ProcessorError, utils::to_pascal_case};
use serde::{Serialize, Serializer};
use std::hash::{Hash, Hasher};
use std::{collections::HashMap, fmt::Display};

#[derive(Debug, Clone, Eq)]
pub enum IdOpt {
    String(String),
    // #[serde(serialize_with = "replacement_serialize")]
    ReplacementMap {
        original_id: String,
        replacement_id: String,
    },
}

impl Hash for IdOpt {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            IdOpt::String(ref s) => s.hash(state),
            IdOpt::ReplacementMap {
                ref replacement_id, ..
            } => {
                replacement_id.hash(state);
            }
        }
    }
}

impl PartialEq for IdOpt {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (IdOpt::String(ref s1), IdOpt::String(ref s2)) => s1 == s2,
            (
                IdOpt::ReplacementMap {
                    replacement_id: ref r1,
                    ..
                },
                IdOpt::ReplacementMap {
                    replacement_id: ref r2,
                    ..
                },
            ) => r1 == r2,
            _ => false,
        }
    }
}

impl IdOpt {
    pub fn final_iri(&self) -> String {
        match self {
            IdOpt::String(ref s) => s.clone(),
            IdOpt::ReplacementMap {
                replacement_id,
                original_id: _,
            } => replacement_id.clone(),
        }
    }

    pub fn to_pascal_case(&self) -> Self {
        match self {
            IdOpt::String(ref s) => IdOpt::String(to_pascal_case(s)),
            IdOpt::ReplacementMap {
                original_id,
                replacement_id,
            } => IdOpt::ReplacementMap {
                original_id: original_id.clone(),
                replacement_id: to_pascal_case(replacement_id),
            },
        }
    }

    pub fn with_base_iri(&self, base_iri: &str) -> Self {
        match self {
            IdOpt::String(ref s) => {
                if !s.starts_with("http") {
                    IdOpt::String(format!("{}{}", base_iri, s))
                } else {
                    self.clone()
                }
            }
            IdOpt::ReplacementMap {
                ref replacement_id,
                ref original_id,
            } => {
                if !replacement_id.starts_with("http") {
                    IdOpt::ReplacementMap {
                        replacement_id: format!("{}{}", base_iri, replacement_id),
                        original_id: original_id.clone(),
                    }
                } else {
                    self.clone()
                }
            }
        }
    }
}

impl Display for IdOpt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdOpt::String(ref s) => write!(f, "{}", s),
            IdOpt::ReplacementMap {
                ref original_id,
                ref replacement_id,
            } => write!(f, "{} (to be replaced by {})", original_id, replacement_id),
        }
    }
}

// Implement the enum and its custom serialization
impl Serialize for IdOpt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            IdOpt::String(ref s) => serializer.serialize_str(s),
            IdOpt::ReplacementMap {
                ref replacement_id, ..
            } => serializer.serialize_str(replacement_id),
        }
    }
}

#[derive(Debug, Clone)]
pub struct VocabularyTerm {
    pub id: IdOpt,
    pub type_: Vec<String>,
    pub label: String,
    pub sub_class_of: Option<Vec<String>>,
    pub comment: Option<String>,
    pub domain: Option<Vec<String>>,
    pub range: Option<Vec<String>>,
    pub extra_items: HashMap<String, String>,
}

impl VocabularyTerm {
    pub fn update_with(&mut self, other_entry: VocabularyTerm) -> Result<(), ProcessorError> {
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

        // Merge domains if different
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

        // Merge ranges if different
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

        // Merge extra items
        // let extra_property_items = &other_entry.extra_items.iter().filter(|)

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
        let id_value = match self.id {
            IdOpt::String(ref s) => serde_json::Value::String(s.clone()),
            IdOpt::ReplacementMap {
                ref replacement_id, ..
            } => serde_json::Value::String(replacement_id.clone()),
        };
        map.insert("@id".to_string(), id_value);
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
        if let Some(sub_class_of) = &self.sub_class_of {
            map.insert(
                "rdfs:subClassOf".to_string(),
                serde_json::Value::Array(
                    sub_class_of
                        .clone()
                        .into_iter()
                        .map(serde_json::Value::String)
                        .collect(),
                ),
            );
        }
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

#[derive(Default, Debug)]
pub struct VocabularyMap {
    pub classes: HashMap<IdOpt, VocabularyTerm>,
    pub properties: HashMap<IdOpt, VocabularyTerm>,
    /// Maps class name to its identifier property term
    /// We use class name <String> because this comes from the manifest directly
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
    pub fn get_identifier_label(&self, class_name: &String) -> Option<&str> {
        self.identifiers
            .get(class_name)
            .map(|term| term.label.as_str())
    }
}
