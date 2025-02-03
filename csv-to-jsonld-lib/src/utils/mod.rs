use std::str::FromStr;

use crate::{types::PropertyDatatype, ProcessorError};

pub const DATE_FORMATS: [&str; 15] = [
    "%Y-%m-%d",          // 2024-06-17
    "%Y/%m/%d",          // 2024/06/17
    "%m-%d-%Y",          // 06-17-2024
    "%m/%d/%Y",          // 06/17/2024
    "%d-%m-%Y",          // 17-06-2024
    "%d/%m/%Y",          // 17/06/2024
    "%Y-%m-%d %H:%M:%S", // 2024-06-17 12:30:00
    "%b %d, %Y",         // Jun 17, 2024
    "%B %d, %Y",         // June 17, 2024
    "%Y",                // 2024 (assume Jan 1 by default)
    "%Y-%m",             // 2024-06 (assume the first day of the month)
    "%Y/%m",             // 2024/06 (assume the first day of the month)
    "%b %Y",             // Jun 2024 (assume the first day of the month)
    "%B %Y",             // June 2024 (assume the first day of the month)
    "%m-%Y",             // 06-2024 (assume the first day of the month)
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

pub fn is_valid_url(url: &str) -> bool {
    url::Url::parse(url).is_ok()
}

/// Expands a relative IRI with a base IRI. Only expands if the relative IRI does not already start with a scheme
pub fn expand_iri_with_base(base_iri: &str, possibly_relative_iri: &str) -> String {
    // Attempt to parse the base IRI
    let mut base_url = match url::Url::parse(base_iri) {
        Ok(url) => url,
        Err(_) => return possibly_relative_iri.to_string(),
    };

    if let Some(fragment) = base_url.fragment() {
        if fragment.is_empty() {
            // Handle special case for empty fragment (e.g., "http://example.com/base#")
            return match url::Url::parse(possibly_relative_iri) {
                Ok(url) => url.to_string(), // Return absolute URL as is
                Err(_) => {
                    base_url.set_fragment(Some(possibly_relative_iri));
                    urlencoding::decode(base_url.as_str()).unwrap().to_string() // Append relative IRI to the fragment
                }
            };
        } else {
            return possibly_relative_iri.to_string(); // Return input as is if fragment is non-empty
        }
    }

    // Standard resolution for relative IRIs
    let result = base_url.join(possibly_relative_iri);
    match result {
        Ok(url) => urlencoding::decode(url.as_str())
            .map(|s| s.to_string())
            .unwrap_or(possibly_relative_iri.to_string()),
        Err(_) => possibly_relative_iri.to_string(), // Return input as is if join fails
    }

    // urlencoding::decode(result)
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
    if is_valid_url(s) {
        return s.to_string();
    }
    let pascal = to_pascal_case(s);
    let mut c = pascal.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_lowercase().chain(c).collect(),
    }
}

/// Convert PascalCase names to kebab-case
pub fn to_kebab_case(s: &str) -> String {
    let mut result = String::new();
    let input = to_pascal_case(s);

    for (i, c) in input.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                result.push('-');
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }

    result
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

#[cfg(test)]
mod tests {

    use std::sync::Once;
    use tracing::info;

    static INIT: Once = Once::new();

    /// Initialize logging exactly once for all tests
    fn init_logging() {
        INIT.call_once(|| {
            tracing_subscriber::fmt()
                .with_test_writer()
                .with_max_level(tracing::Level::DEBUG)
                .init();
        });
    }

    #[test]
    fn test_expand_iri_with_base_and_spaces() {
        init_logging();
        let base = "http://example.com/base/";
        let relative = "resource with spaces";

        info!("Testing expand with valid relative");
        assert_eq!(
            super::expand_iri_with_base(base, relative),
            "http://example.com/base/resource with spaces"
        );
    }

    #[test]
    fn test_expand_iri_with_base() {
        init_logging();

        let base = "http://example.com/base/";
        let relative = "path/to/resource";
        let absolute = "http://example.com/absolute/resource";

        info!("Testing expand with valid relative");
        assert_eq!(
            super::expand_iri_with_base(base, relative),
            "http://example.com/base/path/to/resource"
        );
        info!("Testing expand with valid absolute");
        assert_eq!(
            super::expand_iri_with_base(base, absolute),
            "http://example.com/absolute/resource"
        );

        let base = "s3://example.com/base/";

        info!("Testing expand with S3 base");
        assert_eq!(
            super::expand_iri_with_base(base, relative),
            "s3://example.com/base/path/to/resource"
        );

        let base = "http://example.com/base#";

        info!("Testing expand with base containing fragment");
        assert_eq!(
            super::expand_iri_with_base(base, relative),
            "http://example.com/base#path/to/resource"
        );

        let relative = "prefix:value";

        info!("Testing expand with prefixed relative");
        assert_eq!(super::expand_iri_with_base(base, relative), "prefix:value");

        info!("Testing expand with slash-delimited suffix");

        let relative = "path-to/name with spaces";
        assert_eq!(
            super::expand_iri_with_base(base, relative),
            "http://example.com/base#path-to/name with spaces"
        );
    }
}
