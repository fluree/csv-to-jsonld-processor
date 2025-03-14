use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use csv_to_jsonld::{
    Manifest, ProcessingOutcome, ProcessingState, ProcessorBuilder, StorageLocation,
};
use manifest::{Template, BASIC_MANIFEST, FULL_MANIFEST};
use std::{fs, path::PathBuf};
use tracing::{error, info, warn, Level};

mod manifest;

/// CSV to JSON-LD Processor
/// Converts CSV files to JSON-LD format based on a manifest specification
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Process CSV files according to a manifest
    Process {
        /// Path to the manifest file that specifies the CSV processing configuration
        #[arg(short, long, value_name = "PATH TO MANIFEST")]
        manifest: PathBuf,

        /// Enable strict mode for more rigorous validation
        #[arg(short, long)]
        strict: bool,

        /// Output directory for generated JSON-LD files
        #[arg(short, long, value_name = "OUTPUT DIRECTORY PATH")]
        output: Option<PathBuf>,

        /// Enable verbose output for detailed processing information
        #[arg(short, long)]
        verbose: bool,

        /// Export vocabulary metadata to a file
        #[arg(long, value_name = "VOCABULARY METADATA OUTPUT PATH")]
        export_vocab_meta: Option<StorageLocation>,

        /// Path to vocabulary metadata file to import
        #[arg(long, value_name = "VOCABULARY METADATA INPUT PATH")]
        import_vocab_meta: Option<StorageLocation>,
    },
    /// Generate a manifest template
    GenerateManifest {
        /// Type of manifest template to generate (basic/full)
        #[arg(short = 't', long = "type", default_value = "basic")]
        template_type: String,

        /// Output path for the generated manifest
        #[arg(
            short,
            long,
            default_value = "manifest.jsonc",
            value_name = "OUTPUT PATH"
        )]
        output: PathBuf,

        /// Enable verbose output for detailed processing information
        #[arg(short, long)]
        verbose: bool,
    },
    /// Validate a manifest file against the configuration schema
    Validate {
        /// Path to the manifest file to validate
        #[arg(
            short,
            long,
            default_value = "manifest.jsonc",
            value_name = "PATH TO MANIFEST"
        )]
        manifest: PathBuf,
        /// Enable strict mode for more rigorous validation
        #[arg(short, long, default_value = "false")]
        strict: bool,

        /// Enable verbose output for detailed processing information
        #[arg(short, long)]
        verbose: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let is_verbose = match &cli.command {
        Commands::Process { verbose, .. }
        | Commands::GenerateManifest { verbose, .. }
        | Commands::Validate { verbose, .. } => *verbose,
    };

    // Initialize logging with appropriate level
    let level = if is_verbose {
        Level::DEBUG
    } else {
        Level::INFO
    };

    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .with_file(true)
        .with_line_number(true)
        .init();

    info!("CSV to JSON-LD Processor starting up...");

    match &cli.command {
        Commands::GenerateManifest {
            template_type,
            output,
            ..
        } => generate_manifest_command(template_type, output),
        Commands::Validate {
            manifest, strict, ..
        } => validate_command(manifest, strict),
        Commands::Process {
            manifest,
            strict,
            output,
            export_vocab_meta,
            import_vocab_meta,
            ..
        } => {
            process_command(
                manifest,
                *strict,
                output,
                export_vocab_meta.clone(),
                import_vocab_meta.clone(),
            )
            .await
        }
    }
}

async fn process_command(
    manifest_path: &PathBuf,
    strict: bool,
    output: &Option<PathBuf>,
    export_vocab_meta: Option<StorageLocation>,
    import_vocab_meta: Option<StorageLocation>,
) -> Result<()> {
    let mut processing_state = ProcessingState::new();
    if strict {
        info!("Running in strict mode");
    }

    // Verify manifest file exists
    if !manifest_path.exists() {
        anyhow::bail!("Manifest file not found: {}", manifest_path.display());
    }

    // Get the manifest's parent directory to use as base path
    let base_path = manifest_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Could not determine parent directory of manifest file"))?;

    let output_path = output.clone().unwrap_or_else(|| base_path.to_path_buf());

    // Load and validate manifest
    info!("Loading manifest from {}", manifest_path.display());
    let mut manifest = Manifest::from_file(manifest_path)
        .context("Failed to load manifest. See errors for additional details:")?;

    info!("Validating manifest configuration...");
    processing_state.merge(
        manifest
            .validate(strict)
            .context("Failed to validate manifest")?,
    );

    info!(
        "Manifest '{}' loaded and validated successfully",
        manifest.name
    );
    info!("Description: {}", manifest.description);

    // Create and run processor
    info!("Initializing processor...");

    let is_model_sequence_empty = manifest.model.sequence.is_empty();

    let processor_builder = ProcessorBuilder::from_manifest(manifest)
        .with_base_path(base_path)
        .with_instance_output_path(output_path.to_string_lossy().to_string())?
        .with_model_output_path(output_path.to_string_lossy().to_string())?
        .with_strict(strict)
        .with_export_vocab_meta(export_vocab_meta);

    let processor_builder = if is_model_sequence_empty {
        tracing::info!(
            "No model files specified in manifest, attempting to load vocabulary metadata."
        );
        let meta_path_key = match import_vocab_meta {
            Some(StorageLocation::Local { file_name, .. }) => {
                file_name.to_string_lossy().to_string()
            }
            Some(StorageLocation::S3 { key, .. }) => key.to_string_lossy().to_string(),
            None => "model_metadata.tmp".to_string(),
        };

        let vocab_meta_path = base_path.join(meta_path_key);
        processor_builder
            .with_vocab_meta_path(vocab_meta_path.to_string_lossy().to_string())
            .map_err(|e| {
                tracing::error!("Failed to load vocabulary metadata: {}", e);
                processing_state.add_error_from(e);
                let outcome = ProcessingOutcome::from_state(processing_state.clone());
                outcome.report().unwrap_err()
            })?
    } else {
        processor_builder
    };

    let mut processor = processor_builder.build().await?;

    let proto_outcome = ProcessingOutcome::from_state(processing_state);

    let processing_outcome = processor
        .process()
        .await
        .context("Failed to process CSV files")?;

    let final_outcome = proto_outcome.merge_outcome(processing_outcome);

    final_outcome.report()?;

    Ok(())
}

fn generate_manifest_command(template_type: &str, output: &PathBuf) -> Result<()> {
    let template_path = match template_type.to_lowercase().as_str() {
        "basic" => Template::Basic,
        "full" => Template::Full,
        _ => anyhow::bail!("Invalid template type. Must be either 'basic' or 'full'"),
    };

    info!("Generating {} manifest template...", template_type);

    // Read the template file
    let template_content = match template_path {
        Template::Basic => BASIC_MANIFEST,
        Template::Full => FULL_MANIFEST,
    };

    // if output is a directory, append the default file name
    let full_file_output_path = if output.is_dir() {
        output.join("manifest.jsonc")
    } else {
        output.into()
    };

    // Write the template to the output file
    fs::write(&full_file_output_path, template_content)
        .context(format!("Failed to write manifest to: {}", output.display()))?;

    info!(
        "Successfully generated manifest template at: {}",
        full_file_output_path.display()
    );
    Ok(())
}

fn validate_command(manifest_path: &PathBuf, is_strict: &bool) -> Result<()> {
    info!("Validating manifest...");

    // Verify manifest file exists
    if !manifest_path.exists() {
        anyhow::bail!(
            "Manifest file not found: {}. Try using --manifest <PATH TO MANIFEST>",
            manifest_path.display()
        );
    }

    // Attempt to deserialize the manifest to validate it
    let mut manifest = Manifest::from_file(manifest_path)
        .context("Failed to parse manifest. See errors for additional details:")?;

    // Run additional validation checks
    manifest
        .validate(*is_strict)
        .context("Failed to validate manifest")?;

    info!("Manifest validation successful");
    info!("Name: {}", manifest.name);
    info!("Description: {}", manifest.description);
    Ok(())
}
