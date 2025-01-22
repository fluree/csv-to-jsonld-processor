pub enum Template {
    Basic,
    Full,
}

pub const BASIC_MANIFEST: &str = r#"{
  // Model section defines CSV paths and configuration for vocabulary/model processing
  // If no "model" section is present, default assumptions will be made about instance CSV files
  "model": {
    // Sequence for processing model CSV files
    // Values must either be the string path to each CSV file or an object with additional configuration for each file
    "sequence": ["model/vocabulary.csv"]
  },
  // Instances section defines CSV paths and configuration for instance data processing
  "instances": {
    // Sequence for processing instance CSV files
    // Values must either be the string path to each CSV file or an object with additional configuration for each file
    "sequence": ["data/instances.csv"]
  }
}"#;

pub const FULL_MANIFEST: &str = r#"{
  "@context": {
    "@vocab": "https://ns.flur.ee/imports#",
    "sequence": {
      "@id": "https://ns.flur.ee/imports#sequence",
      "@container": "@list",
      "@type": "@id"
    }
  },
  "@type": "CSVImportManifest",
  // The top level data of "@id", "name", and "description" will be used as metadata for the generated vocabulary entity
  "@id": "your-model-id",
  "name": "Your Model Name",
  "description": "Description of your data model",
  // Model section defines CSV paths and configuration for vocabulary/model processing
  // If no "model" section is present, default assumptions will be made about instance CSV files
  "model": {
    // Base IRI for model terms (optional)
    // All generated IRIs will be prefixed with this (e.g. "http://example.org/terms/MyCustomClass")
    "baseIRI": "http://example.org/terms/",
    // Whether to namespace IRIs (optional)
    // When true, IRIs will include the full path structure
    "namespaceIris": true,
    // Base path for model CSV files (optional)
    // All paths in sequence will be relative to this
    "path": "model/",
    // Sequence for processing model CSV files
    // Values must either be the string path to each CSV file or an object with additional configuration for each file
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
