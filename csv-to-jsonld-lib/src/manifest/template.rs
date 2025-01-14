pub const BASIC_MANIFEST: &str = r#"{
  // Standard JSON-LD context - defines the vocabulary and sequence container
  "@context": {
    "@vocab": "https://ns.flur.ee/imports#",
    "sequence": {
      "@id": "https://ns.flur.ee/imports#sequence",
      "@container": "@list"
    }
  },
  // Must be CSVImportManifest
  "@type": "CSVImportManifest",
  // Unique identifier for this manifest
  "@id": "your-model-id",
  // Human-readable name
  "name": "Your Model Name",
  // Description of what this manifest processes
  "description": "Description of your data model",
  
  // Model section defines how to process vocabulary/model CSV files
  "model": {
    // Sequence of steps to process model CSV files
    "sequence": [
      {
        // Path to CSV file containing class and property definitions
        "path": "model/vocabulary.csv",
        // BasicVocabularyStep processes basic class and property definitions
        "@type": [
          "BasicVocabularyStep"
        ],
        // Map CSV column names to standard vocabulary fields
        "overrides": [
          {
            // The CSV column containing class names
            "column": "Class",
            // Map to the class identifier field
            "mapTo": "$Class.ID"
          },
          {
            // The CSV column containing property names
            "column": "Property",
            // Map to the property identifier field
            "mapTo": "$Property.ID"
          }
        ]
      }
    ]
  },
  
  // Instances section defines how to process data CSV files
  "instances": {
    // Sequence of steps to process instance CSV files
    "sequence": [
      {
        // Path to CSV file containing instance data
        "path": "data/instances.csv",
        // BasicInstanceStep processes straightforward instance data
        "@type": [
          "BasicInstanceStep"
        ],
        // The type to assign to instances from this CSV
        "instanceType": "YourType"
      }
    ]
  }
}"#;
