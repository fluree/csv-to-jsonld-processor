use crate::utils::{are_conflicting, expand_iri_with_base, normalize_label_for_iri, to_camel_case};
use crate::{error::ProcessorError, utils::to_pascal_case};
use anyhow::Result;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::hash_map::Entry;
use std::hash::{Hash, Hasher};
use std::{collections::HashMap, fmt::Display};

use super::csv::StrictPropertyDatatype;
use super::PropertyDatatype;

#[derive(Debug, Clone, Eq, Deserialize)]
pub enum IdOpt {
    String(String),
    // #[serde(serialize_with = "replacement_serialize")]
    ReplacementMap {
        original_id: String,
        replacement_id: String,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum StrictIdOpt {
    String(String),
    ReplacementMap {
        original_id: String,
        replacement_id: String,
    },
}

impl From<IdOpt> for StrictIdOpt {
    fn from(id: IdOpt) -> Self {
        match id {
            IdOpt::String(s) => StrictIdOpt::String(s),
            IdOpt::ReplacementMap {
                original_id,
                replacement_id,
            } => StrictIdOpt::ReplacementMap {
                original_id,
                replacement_id,
            },
        }
    }
}

impl From<StrictIdOpt> for IdOpt {
    fn from(id: StrictIdOpt) -> Self {
        match id {
            StrictIdOpt::String(s) => IdOpt::String(s),
            StrictIdOpt::ReplacementMap {
                original_id,
                replacement_id,
            } => IdOpt::ReplacementMap {
                original_id,
                replacement_id,
            },
        }
    }
}

impl Hash for IdOpt {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            IdOpt::String(ref s) => s.hash(state),
            IdOpt::ReplacementMap {
                ref original_id, ..
            } => {
                original_id.hash(state);
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
                    original_id: ref o1,
                    replacement_id: ref r1,
                },
                IdOpt::ReplacementMap {
                    original_id: ref o2,
                    replacement_id: ref r2,
                },
            ) => o1 == o2 || r1 == r2,
            (
                IdOpt::String(ref s),
                IdOpt::ReplacementMap {
                    original_id: ref o,
                    replacement_id: ref r,
                },
            ) => s == r || s == o,
            (
                IdOpt::ReplacementMap {
                    original_id: ref o,
                    replacement_id: ref r,
                },
                IdOpt::String(ref s),
            ) => s == r || s == o,
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

    pub fn normalize(&self) -> Self {
        match self {
            IdOpt::String(ref s) => IdOpt::String(normalize_label_for_iri(s)),
            IdOpt::ReplacementMap {
                original_id,
                replacement_id,
            } => IdOpt::ReplacementMap {
                original_id: original_id.clone(),
                replacement_id: normalize_label_for_iri(replacement_id),
            },
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

    pub fn to_camel_case(&self) -> Self {
        match self {
            IdOpt::String(ref s) => IdOpt::String(to_camel_case(s)),
            IdOpt::ReplacementMap {
                original_id,
                replacement_id,
            } => IdOpt::ReplacementMap {
                original_id: original_id.clone(),
                replacement_id: to_camel_case(replacement_id),
            },
        }
    }

    pub fn with_base_iri(&self, base_iri: &str) -> Self {
        match self {
            IdOpt::String(ref s) => IdOpt::String(expand_iri_with_base(base_iri, s)),
            IdOpt::ReplacementMap {
                ref replacement_id,
                ref original_id,
            } => IdOpt::ReplacementMap {
                replacement_id: expand_iri_with_base(base_iri, replacement_id),
                original_id: original_id.clone(),
            },
        }
    }

    pub fn without_base_iri(&self, base_iri: &str) -> Self {
        match self {
            IdOpt::String(ref s) => {
                if s.starts_with(base_iri) {
                    IdOpt::String(s.replace(base_iri, ""))
                } else {
                    self.clone()
                }
            }
            IdOpt::ReplacementMap {
                ref replacement_id,
                ref original_id,
            } => {
                if replacement_id.starts_with(base_iri) {
                    IdOpt::ReplacementMap {
                        replacement_id: replacement_id.replace(base_iri, ""),
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

#[derive(Debug, Clone, Deserialize)]
pub struct VocabularyTerm {
    pub id: IdOpt,
    pub type_: Vec<String>,
    pub label: Option<String>,
    pub sub_class_of: Option<Vec<String>>,
    pub comment: Option<String>,
    pub domain: Option<Vec<String>>,
    pub range: Option<Vec<PropertyDatatype>>,
    pub extra_items: HashMap<String, String>,
    pub one_of: Option<Vec<IdOpt>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StrictVocabularyTerm {
    pub id: StrictIdOpt,
    pub type_: Vec<String>,
    pub label: Option<String>,
    pub sub_class_of: Option<Vec<String>>,
    pub comment: Option<String>,
    pub domain: Option<Vec<String>>,
    pub range: Option<Vec<StrictPropertyDatatype>>,
    pub extra_items: HashMap<String, String>,
    pub one_of: Option<Vec<StrictIdOpt>>,
}

impl From<VocabularyTerm> for StrictVocabularyTerm {
    fn from(term: VocabularyTerm) -> Self {
        let id = term.id.into();
        let range = term
            .range
            .map(|range| range.into_iter().map(Into::into).collect());
        let one_of = term
            .one_of
            .map(|one_of| one_of.into_iter().map(Into::into).collect());
        Self {
            id,
            type_: term.type_,
            label: term.label,
            sub_class_of: term.sub_class_of,
            comment: term.comment,
            domain: term.domain,
            range,
            extra_items: term.extra_items,
            one_of,
        }
    }
}

impl From<StrictVocabularyTerm> for VocabularyTerm {
    fn from(term: StrictVocabularyTerm) -> Self {
        let id = term.id.into();
        let range = term
            .range
            .map(|range| range.into_iter().map(Into::into).collect());
        let one_of = term
            .one_of
            .map(|one_of| one_of.into_iter().map(Into::into).collect());
        Self {
            id,
            type_: term.type_,
            label: term.label,
            sub_class_of: term.sub_class_of,
            comment: term.comment,
            domain: term.domain,
            range,
            extra_items: term.extra_items,
            one_of,
        }
    }
}

impl VocabularyTerm {
    pub fn update_with(&mut self, other_entry: VocabularyTerm) -> Result<(), ProcessorError> {
        if are_conflicting(&self.label, &other_entry.label) {
            tracing::debug!("CONFLICT LABEL!\n{:#?}\n\nvs\n\n{:#?}", self, other_entry);
            return Err(ProcessorError::Processing(format!(
                "The CSV uses conflicting labels for the same term '{}':\n\
                     - Label 1: {}\n\
                     - Label 2: {}",
                self.id,
                self.label.as_ref().unwrap(),
                other_entry.label.unwrap()
            )));
        }

        if self.label.is_none() {
            self.label = Some(other_entry.label.clone().unwrap())
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
                if this_value != value && !this_value.is_empty() && !value.is_empty() {
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
        if let Some(label) = &self.label {
            map.insert(
                "rdfs:label".to_string(),
                serde_json::Value::String(label.clone()),
            );
        }
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
                        .map(|value| serde_json::to_value(&value).unwrap())
                        .collect(),
                ),
            );
        }
        for (key, value) in &self.extra_items {
            if !value.is_empty() {
                map.insert(key.clone(), serde_json::Value::String(value.clone()));
            }
        }
        if let Some(one_of) = &self.one_of {
            map.insert(
                "f:oneOf".to_string(),
                serde_json::Value::Array(
                    one_of
                        .clone()
                        .into_iter()
                        .map(|value| serde_json::Value::String(value.final_iri()))
                        .collect(),
                ),
            );
        }
        map.serialize(serializer)
    }
}

#[derive(Debug, Serialize)]
pub struct FlureeDataModel {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "rdfs:label")]
    pub label: String,
    #[serde(rename = "rdfs:comment")]
    pub comment: String,
    #[serde(rename = "@type")]
    pub type_: Vec<String>,
    #[serde(rename(serialize = "f:properties"))]
    pub properties: Vec<VocabularyTerm>,
    #[serde(rename(serialize = "f:classes"))]
    pub classes: Vec<VocabularyTerm>,
}

#[derive(Debug, Serialize)]
pub struct JsonLdVocabulary {
    #[serde(rename = "@context")]
    pub context: serde_json::Value,
    pub ledger: String,
    pub insert: FlureeDataModel,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct VocabularyMap {
    pub classes: HashMap<IdOpt, VocabularyTerm>,
    pub properties: HashMap<IdOpt, VocabularyTerm>,
    /// Maps class name to its identifier property term
    /// We use class name <String> because this comes from the manifest directly
    pub identifiers: HashMap<String, VocabularyTerm>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StrictVocabularyMap {
    pub classes: Vec<(StrictIdOpt, StrictVocabularyTerm)>,
    pub properties: Vec<(StrictIdOpt, StrictVocabularyTerm)>,
    pub identifiers: HashMap<String, StrictVocabularyTerm>,
}

impl From<VocabularyMap> for StrictVocabularyMap {
    fn from(vocabulary_map: VocabularyMap) -> Self {
        let classes = vocabulary_map
            .classes
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        let properties = vocabulary_map
            .properties
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        let identifiers = vocabulary_map
            .identifiers
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect();
        Self {
            classes,
            properties,
            identifiers,
        }
    }
}

impl From<StrictVocabularyMap> for VocabularyMap {
    fn from(vocabulary_map: StrictVocabularyMap) -> Self {
        let classes = vocabulary_map
            .classes
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        let properties = vocabulary_map
            .properties
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        let identifiers = vocabulary_map
            .identifiers
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect();
        Self {
            classes,
            properties,
            identifiers,
        }
    }
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
    pub fn get_identifier_label(&self, class_name: &String) -> Option<&String> {
        self.identifiers
            .get(class_name)
            .map(|term| term.label.as_ref().unwrap())
    }

    pub fn update_or_insert_picklist_instance(
        &mut self,
        class_name: String,
        instance_id: IdOpt,
    ) -> Result<(), ProcessorError> {
        tracing::trace!(
            "Updating vocabulary with picklist instance: {} for class: {}",
            instance_id,
            class_name
        );
        // let vocab_entry = self.classes.entry(IdOpt::String(class_name.clone()));
        let class_key = self
            .classes
            .keys()
            .find(|id| {
                let final_id = id.normalize().to_pascal_case();
                match final_id {
                    IdOpt::String(string_id) => string_id == class_name,
                    IdOpt::ReplacementMap { original_id, .. } => original_id == class_name,
                }
            })
            .ok_or_else(|| {
                ProcessorError::Processing(format!(
                    "Cannot process picklist entry because class name ({}) not found in vocabulary classes",
                    class_name
                ))
            })?;
        let vocab_entry = self.classes.entry(class_key.clone());
        match vocab_entry {
            Entry::Occupied(mut entry) => {
                let term = entry.get_mut();
                if let Some(one_of) = &mut term.one_of {
                    one_of.push(instance_id);
                } else {
                    term.one_of = Some(vec![instance_id]);
                };
            }
            _ => {
                panic!(
                    "Class name not found in vocabulary identifiers: {}",
                    class_name
                );
            }
        }
        Ok(())
    }
}
