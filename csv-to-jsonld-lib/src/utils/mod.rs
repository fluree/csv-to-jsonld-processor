use std::str::FromStr;

use crate::{types::PropertyDatatype, ProcessorError};

pub const DATE_FORMATS: [&str; 9] = [
    "%Y-%m-%d",          // 2024-06-17
    "%Y/%m/%d",          // 2024/06/17
    "%m-%d-%Y",          // 06-17-2024
    "%m/%d/%Y",          // 06/17/2024
    "%d-%m-%Y",          // 17-06-2024
    "%d/%m/%Y",          // 17/06/2024
    "%Y-%m-%d %H:%M:%S", // 2024-06-17 12:30:00
    "%b %d, %Y",         // Jun 17, 2024
    "%B %d, %Y",         // June 17, 2024
];

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

/// Normalize a string to be used as an IRI label
pub fn normalize_label_for_iri(label: &str) -> String {
    label
        .trim() // Trim leading/trailing spaces
        .replace("//", "-") // Replace double slashes
        .replace(|c: char| !c.is_alphanumeric() && c != '-', "-") // Replace invalid chars with '-'
        .split('-') // Prevent multiple consecutive dashes
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// Convert a string to camelCase
pub fn to_camel_case(s: &str) -> String {
    let pascal = to_pascal_case(s);
    let mut c = pascal.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_lowercase().chain(c).collect(),
    }
}

// pub fn get_hash_id<T: Hash>(entity: T) -> String {
//     let mut hasher = DefaultHasher::new();
//     entity.hash(&mut hasher);
//     hasher.finish().to_string()
// }

/// Map CSV type to XSD type
pub fn map_xsd_type(csv_type: &str) -> Result<PropertyDatatype, ProcessorError> {
    PropertyDatatype::from_str(csv_type)
    // let result = match csv_type.to_lowercase().as_str() {
    //     "@id" => CsvDatatype::ID,
    //     "uri" => CsvDatatype::URI,
    //     "string" => CsvDatatype::String,
    //     "float" => CsvDatatype::Decimal,
    //     "integer" => CsvDatatype::Integer,
    //     "date" => CsvDatatype::Date,
    //     _ => {
    //         return Err(ProcessorError::InvalidManifest(format!(
    //             "Invalid CSV datatype: {} [Expected: @id, URI, String, Float, Integer, Date]",
    //             csv_type
    //         )))
    //     }
    // };
    // Ok(result)
}

/// Check if two Option values are conflicting (both Some but different values)
pub fn are_conflicting(value_one: &Option<String>, value_two: &Option<String>) -> bool {
    match (value_one, value_two) {
        (Some(value_one), Some(value_two)) => {
            if value_one.is_empty() || value_two.is_empty() {
                false
            } else {
                value_one != value_two
            }
        }
        (None, None) => false,
        _ => false,
    }
}

/// Merge two HashMaps, returning an error if there are conflicting values
// pub fn merge_maps<K, V>(
//     map1: &mut HashMap<K, V>,
//     map2: HashMap<K, V>,
//     error_msg: &str,
// ) -> Result<(), String>
// where
//     K: Eq + std::hash::Hash + Clone,
//     V: PartialEq + Clone + Debug,
// {
//     for (key, value) in map2 {
//         if let Some(existing) = map1.get(&key) {
//             if existing != &value {
//                 return Err(format!("{}: {:?} vs {:?}", error_msg, existing, value));
//             }
//         } else {
//             map1.insert(key, value);
//         }
//     }
//     Ok(())
// }

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
