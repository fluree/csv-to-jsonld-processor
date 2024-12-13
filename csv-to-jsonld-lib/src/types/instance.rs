use std::collections::HashSet;

use serde::Serialize;
use serde_json::{Map, Value};

use crate::ProcessorError;

#[derive(Debug, Serialize, Clone)]
pub struct JsonLdInstance {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "@type")]
    pub type_: Vec<String>,
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
                        set.insert(value);
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
    #[serde(rename = "@context")]
    pub context: Map<String, serde_json::Value>,
    #[serde(rename = "@graph")]
    pub graph: Vec<JsonLdInstance>,
}

#[derive(Debug, Serialize)]
pub struct JsonLdContext {
    #[serde(rename = "@context")]
    pub context: Map<String, serde_json::Value>,
}
