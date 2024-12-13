use std::{collections::HashMap, fmt::Debug};

use crate::ProcessorError;

/// Convert a string to PascalCase
pub fn to_pascal_case(s: &str) -> String {
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

/// Convert a string to camelCase
pub fn to_camel_case(s: &str) -> String {
    // Special handling for B/(W) -> BW
    let s = s.replace("B/(W)", "BW");

    let pascal = to_pascal_case(&s);
    let mut c = pascal.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_lowercase().chain(c).collect(),
    }
}

/// Map CSV type to XSD type
pub fn map_xsd_type(csv_type: &str) -> String {
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

/// Check if two Option values are conflicting (both Some but different values)
pub fn are_conflicting<T>(value_one: &Option<T>, value_two: &Option<T>) -> bool
where
    T: PartialEq,
{
    match (value_one, value_two) {
        (Some(value_one), Some(value_two)) => value_one != value_two,
        (None, None) => false,
        _ => false,
    }
}

/// Merge two HashMaps, returning an error if there are conflicting values
pub fn merge_maps<K, V>(
    map1: &mut HashMap<K, V>,
    map2: HashMap<K, V>,
    error_msg: &str,
) -> Result<(), String>
where
    K: Eq + std::hash::Hash + Clone,
    V: PartialEq + Clone + Debug,
{
    for (key, value) in map2 {
        if let Some(existing) = map1.get(&key) {
            if existing != &value {
                return Err(format!("{}: {:?} vs {:?}", error_msg, existing, value));
            }
        } else {
            map1.insert(key, value);
        }
    }
    Ok(())
}

pub fn validate_column_identifier(term: String) -> Result<String, ProcessorError> {
    if let Some(rest) = term.strip_prefix('$') {
        Ok(rest.to_string())
    } else {
        Err(ProcessorError::InvalidManifest(
            "Override mapTo value must start with '$'".to_string(),
        ))
    }
}

#[macro_export]
macro_rules! contains_variant {
    ($collection:expr, $pattern:pat) => {
        $collection.iter().any(|item| matches!(item, $pattern))
    };
}
