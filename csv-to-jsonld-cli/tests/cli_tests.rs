use std::process::Command;
use std::sync::Once;
use tracing::{error, info};

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
fn test_cli() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    info!("Starting CLI test");

    let output = Command::new("cargo")
        .arg("run")
        .arg("--")
        .arg("--manifest")
        .arg("/Users/ajohnson/fluree/rust/csv-to-jsonld-processor/test-data/manifest.jsonld")
        .arg("--strict")
        .arg("--output")
        .arg("/Users/ajohnson/fluree/rust/csv-to-jsonld-processor/csv-to-jsonld-cli/tests/output/")
        .output()?;

    if !output.status.success() {
        error!("Command failed with status: {}", output.status);
        error!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        error!("stdout: {}", String::from_utf8_lossy(&output.stdout));
    } else {
        info!("Command executed successfully");
        if !output.stderr.is_empty() {
            info!("stderr output: {}", String::from_utf8_lossy(&output.stderr));
        }
        info!("stdout: {}", String::from_utf8_lossy(&output.stdout));
    }

    assert!(output.status.success());
    // assert!(output.stderr.is_empty());

    info!("Comparing vocabulary output files");
    let expected_vocab_output = std::fs::read_to_string(
        "/Users/ajohnson/fluree/rust/csv-to-jsonld-processor/test-data/vocabulary.jsonld",
    )?;

    let actual_vocab_output = std::fs::read_to_string(
        "/Users/ajohnson/fluree/rust/csv-to-jsonld-processor/csv-to-jsonld-cli/tests/output/vocabulary.jsonld",
    )?;

    if expected_vocab_output != actual_vocab_output {
        error!("Vocabulary files do not match");
        error!("Expected: {}", expected_vocab_output);
        error!("Actual: {}", actual_vocab_output);
    }
    info!("Vocabulary files match");

    info!("Comparing instance output files");
    let expected_instance_output = std::fs::read_to_string(
        "/Users/ajohnson/fluree/rust/csv-to-jsonld-processor/test-data/instances.jsonld",
    )?;

    let actual_instance_output = std::fs::read_to_string(
        "/Users/ajohnson/fluree/rust/csv-to-jsonld-processor/csv-to-jsonld-cli/tests/output/instances.jsonld",
    )?;

    if expected_instance_output != actual_instance_output {
        error!("Instance files do not match");
        error!("Expected: {}", expected_instance_output);
        error!("Actual: {}", actual_instance_output);
    }
    info!("Instance files match");

    info!("Test completed successfully");
    Ok(())
}
