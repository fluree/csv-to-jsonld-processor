use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use csv_to_jsonld::{Manifest, Processor};
use manifest::{Template, BASIC_MANIFEST, FULL_MANIFEST};
use std::{fs, path::PathBuf};
use tracing::{info, Level};

mod manifest;

/// CSV to JSON-LD Processor
/// Converts CSV files to JSON-LD format based on a manifest specification
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Enable verbose output for detailed processing information
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Process CSV files according to a manifest
    Process {
        /// Path to the manifest file that specifies the CSV processing configuration
        #[arg(short, long)]
        manifest: PathBuf,

        /// Enable strict mode for more rigorous validation
        #[arg(short, long)]
        strict: bool,

        /// Output directory for generated JSON-LD files
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// Generate a manifest template
    GenerateManifest {
        /// Type of manifest template to generate (basic/full)
        #[arg(short, long, default_value = "basic", ren)]
        type_: String,

        /// Output path for the generated manifest
        #[arg(short, long, default_value = "manifest.jsonc")]
        output: PathBuf,
    },
    /// Validate a manifest file against the configuration schema
    Validate {
        /// Path to the manifest file to validate
        #[arg(short, long, default_value = "manifest.jsonc")]
        manifest: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging with appropriate level
    let level = if cli.verbose {
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
        } => generate_manifest_command(template_type, output),
        Commands::Validate { manifest } => validate_command(manifest),
        Commands::Process {
            manifest,
            strict,
            output,
        } => process_command(manifest, *strict, output).await,
    }
}

async fn process_command(
    manifest_path: &PathBuf,
    strict: bool,
    output: &Option<PathBuf>,
) -> Result<()> {
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
    let manifest = Manifest::from_file(manifest_path)
        .context("Failed to load manifest. See errors for additional details:")?;

    info!("Validating manifest configuration...");
    manifest.validate().context("Failed to validate manifest")?;

    info!(
        "Manifest '{}' loaded and validated successfully",
        manifest.name
    );
    info!("Description: {}", manifest.description);

    // Create and run processor
    info!("Initializing processor...");
    let mut processor =
        Processor::with_base_path(manifest, base_path, strict, output_path.as_path());

    info!("Beginning CSV processing...");
    processor
        .process()
        .await
        .context("Failed to process CSV files")?;

    info!("Processing completed successfully");
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

    // Write the template to the output file
    fs::write(output, template_content)
        .context(format!("Failed to write manifest to: {}", output.display()))?;

    info!(
        "Successfully generated manifest template at: {}",
        output.display()
    );
    Ok(())
}

fn validate_command(manifest_path: &PathBuf) -> Result<()> {
    info!("Validating manifest...");

    // Verify manifest file exists
    if !manifest_path.exists() {
        anyhow::bail!("Manifest file not found: {}", manifest_path.display());
    }

    // Attempt to deserialize the manifest to validate it
    let manifest = Manifest::from_file(manifest_path)
        .context("Failed to parse manifest. See errors for additional details:")?;

    // Run additional validation checks
    manifest.validate().context("Failed to validate manifest")?;

    info!("Manifest validation successful");
    info!("Name: {}", manifest.name);
    info!("Description: {}", manifest.description);
    Ok(())
}
