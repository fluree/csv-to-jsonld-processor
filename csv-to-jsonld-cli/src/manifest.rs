pub enum Template {
    Basic,
    Full,
}

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

pub const FULL_MANIFEST: &str = r#"{
  // Standard JSON-LD context - defines the vocabulary and sequence container
  "@context": {
    "@vocab": "https://ns.flur.ee/imports#",
    "sequence": {
      "@id": "https://ns.flur.ee/imports#sequence",
      "@container": "@list"
    }
  },
  "@type": "CSVImportManifest",
  "@id": "your-model-id",
  "name": "Your Model Name",
  "description": "Description of your data model",
  
  // Model section - defines vocabulary/model processing
  "model": {
    // Base IRI for model terms (optional)
    // All generated IRIs will be prefixed with this
    "baseIRI": "http://example.com/terms/",
    // Whether to namespace IRIs (optional)
    // When true, IRIs will include the full path structure
    "namespaceIris": true,
    // Base path for model CSV files (optional)
    // All paths in sequence will be relative to this
    "path": "model/",
    "sequence": [
      {
        // Basic vocabulary step - processes class and property definitions
        "path": "vocabulary.csv",
        "@type": [
          "CSVImportStep",
          "BasicVocabularyStep"
        ],
        // Map CSV columns to standard vocabulary fields
        "overrides": [
          {
            "column": "Class Name",
            // Available class mappings:
            // $Class.ID - The class identifier
            // $Class.Name - Human-readable name
            // $Class.Description - Class description
            "mapTo": "$Class.ID"
          },
          {
            "column": "Property Name",
            // Available property mappings:
            // $Property.ID - The property identifier
            // $Property.Name - Human-readable name
            // $Property.Description - Property description
            // $Property.Type - Property datatype
            // $Property.TargetClass - Class that this property references
            "mapTo": "$Property.ID"
          }
        ],
        // Additional field mappings beyond standard fields
        "extraItems": [
          {
            "column": "Additional Field",
            // The IRI to map this field to
            "mapTo": "http://example.com/terms/additionalField",
            // Whether this applies to classes or properties
            "onEntity": "PROPERTY",  // or "CLASS"
            // Optional static value instead of using column value
            "value": "optional static value"
          }
        ]
      },
      {
        // Subclass vocabulary step - processes class hierarchy
        "path": "subclasses.csv",
        "@type": [
          "CSVImportStep",
          "SubClassVocabularyStep"
        ],
        // Parent classes that all classes in this CSV inherit from
        "subClassOf": [
          "http://example.com/terms/ParentClass"
        ],
        // Replace auto-generated class IDs with values from this field
        "replaceClassIdWith": "$Class.Name",
        // Additional fields specific to subclasses
        "extraItems": [
          {
            "column": "Category",
            "mapTo": "http://example.com/terms/category",
            "onEntity": "CLASS"
          }
        ]
      },
      {
        // Properties vocabulary step - processes property definitions
        "path": "properties.csv",
        "@type": [
          "CSVImportStep",
          "PropertiesVocabularyStep"
        ],
        // Replace auto-generated property IDs with values from this field
        "replacePropertyIdWith": "$Property.Name",
        // Columns to ignore during processing
        "ignore": [
          "IgnoreThisColumn"
        ]
      }
    ]
  },
  
  // Instances section - defines instance data processing
  "instances": {
    // Base IRI for instance identifiers (optional)
    // All generated instance IRIs will be prefixed with this
    "baseIRI": "http://example.com/ids/",
    // Whether to namespace IRIs (optional)
    // When true, IRIs will include the full path structure
    "namespaceIris": true,
    // Base path for instance CSV files (optional)
    // All paths in sequence will be relative to this
    "path": "instances/",
    "sequence": [
      {
        // Basic instance step - processes straightforward instance data
        "path": "basic.csv",
        "@type": [
          "CSVImportStep",
          "BasicInstanceStep"
        ],
        // The type to assign to instances from this CSV
        "instanceType": "BasicType",
        // Columns to ignore during processing
        "ignore": [
          "IgnoreThisColumn"
        ]
      },
      {
        // Picklist step - processes enumeration values
        // Use this for columns that should have predefined values
        "path": "picklist.csv",
        "@type": [
          "CSVImportStep",
          "PicklistStep"
        ],
        "instanceType": "PicklistType"
      },
      {
        // Subclass instance step - processes instances with dynamic types
        // Use this when instances should be assigned different types
        // based on a column value
        "path": "subclass.csv",
        "@type": [
          "CSVImportStep",
          "SubClassInstanceStep"
        ],
        "instanceType": "SubClassType",
        // Property that determines the subclass type
        "subClassProperty": "hasSubClass"
      },
      {
        // Properties instance step - processes property values
        // Use this for CSV files that define property values
        "path": "properties.csv",
        "@type": [
          "CSVImportStep",
          "PropertiesInstanceStep"
        ],
        "instanceType": "PropertyType",
        // Split multi-value fields on this character
        // Use this when a single column contains multiple values
        "delimitValuesOn": ","
      },
      {
        // Example of pivot columns - creates new instances from column groups
        // Use this when multiple columns should be converted into
        // separate linked instances
        "path": "pivot.csv",
        "@type": [
          "CSVImportStep",
          "BasicInstanceStep"
        ],
        "instanceType": "PivotType",
        "pivotColumns": [
          {
            // Type for the new instances created from pivot columns
            "instanceType": "PivotItemType",
            // Property linking original instance to pivoted instances
            "newRelationshipProperty": "hasItems",
            // Columns to include in pivoted instances
            "columns": [
              "quantity",
              "reference",
              "category"
            ]
          }
        ]
      }
    ]
  }
}
"#;
