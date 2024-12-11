use anyhow::{Context, Result};
use clap::Parser;
use csv_to_jsonld::{Manifest, Processor};
use std::path::PathBuf;
use tracing::{info, Level};

/// CSV to JSON-LD Processor
/// Converts CSV files to JSON-LD format based on a manifest specification
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to the manifest file that specifies the CSV processing configuration
    #[arg(short, long)]
    manifest: PathBuf,

    /// Enable strict mode for more rigorous validation
    #[arg(short, long)]
    strict: bool,

    /// Enable verbose output for detailed processing information
    #[arg(short, long)]
    verbose: bool,

    /// Output directory for generated JSON-LD files
    #[arg(short, long)]
    output: Option<PathBuf>,
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

    if cli.strict {
        info!("Running in strict mode");
    }

    // Verify manifest file exists
    if !cli.manifest.exists() {
        anyhow::bail!("Manifest file not found: {}", cli.manifest.display());
    }

    // Get the manifest's parent directory to use as base path
    let base_path = cli
        .manifest
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Could not determine parent directory of manifest file"))?;

    let output_path = cli.output.unwrap_or_else(|| base_path.to_path_buf());

    // Load and validate manifest
    info!("Loading manifest from {}", cli.manifest.display());
    let manifest = Manifest::from_file(&cli.manifest)
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
        Processor::with_base_path(manifest, base_path, cli.strict, output_path.as_path());

    info!("Beginning CSV processing...");
    processor
        .process()
        .await
        .context("Failed to process CSV files")?;

    info!("Processing completed successfully");
    Ok(())
}
