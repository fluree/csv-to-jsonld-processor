use std::collections::HashSet;

use serde::Serialize;
use serde_json::{Map, Value};

use crate::ProcessorError;

use super::IdOpt;

#[derive(Debug, Serialize, Clone)]
pub struct JsonLdInstance {
    #[serde(rename = "@id")]
    pub id: IdOpt,
    #[serde(rename = "@type")]
    pub type_: Vec<IdOpt>,
    #[serde(flatten)]
    pub properties: Map<String, serde_json::Value>,
}

impl JsonLdInstance {
    pub fn update_with(&mut self, new_instance: JsonLdInstance) -> Result<(), ProcessorError> {
        // Merge properties
        for (key, value) in new_instance.properties {
            match self.properties.entry(key.clone()) {
                serde_json::map::Entry::Vacant(entry) => {
                    entry.insert(value);
                }
                serde_json::map::Entry::Occupied(mut entry) => {
                    let current: &mut Value = entry.get_mut();
                    if current.is_array() {
                        // Add to existing array
                        let array = current.as_array_mut().unwrap();
                        let mut set: HashSet<Value> = array.drain(..).collect();
                        if matches!(value, Value::Array(_)) {
                            let new_set: HashSet<Value> =
                                value.as_array().unwrap().iter().cloned().collect();
                            set.extend(new_set);
                        } else {
                            set.insert(value);
                        }
                        *array = set.into_iter().collect();
                    } else {
                        // Convert to array with both values
                        let mut set: HashSet<Value> = HashSet::new();
                        set.insert(current.clone());
                        set.insert(value);
                        *current = Value::Array(set.into_iter().collect());
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct JsonLdInstances {
    pub ledger: String,
    #[serde(rename = "@context")]
    pub context: Map<String, serde_json::Value>,
    #[serde(rename = "insert")]
    pub insert: Vec<JsonLdInstance>,
}

#[derive(Debug, Serialize)]
pub struct JsonLdContext {
    #[serde(rename = "@context")]
    pub context: Map<String, serde_json::Value>,
}
