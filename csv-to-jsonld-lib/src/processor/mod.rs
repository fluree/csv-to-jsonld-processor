use crate::error::ProcessorError;
use crate::instance::InstanceManager;
use crate::manifest::{InstanceStep, Manifest, StepType};
use crate::vocabulary::VocabularyManager;
use crate::{contains_variant, ImportStep};
use std::path::PathBuf;
use std::sync::Arc;

pub struct Processor {
    manifest: Arc<Manifest>,
    vocabulary_manager: VocabularyManager,
    instance_manager: InstanceManager,
    base_path: PathBuf,
    is_strict: bool,
    output_path: PathBuf,
}

impl Processor {
    pub fn with_base_path<P: Into<PathBuf>>(
        manifest: Manifest,
        base_path: P,
        is_strict: bool,
        output_path: P,
    ) -> Self {
        let base_path = base_path.into();
        let output_path = output_path.into();
        let manifest = Arc::new(manifest);
        tracing::info!("Creating processor with base path: {:?}", base_path);
        Self {
            vocabulary_manager: VocabularyManager::new(Arc::clone(&manifest), is_strict),
            instance_manager: InstanceManager::new(Arc::clone(&manifest), is_strict),
            base_path,
            output_path,
            manifest,
            is_strict,
        }
    }

    fn model_sequence(&self) -> Vec<crate::manifest::ImportStep> {
        self.manifest.model.sequence.clone()
    }

    fn instance_sequence(&self) -> Vec<crate::manifest::ImportStep> {
        self.manifest.instances.sequence.clone()
    }

    fn resolve_path(&self, relative_path: &str) -> PathBuf {
        self.base_path.join(relative_path)
    }

    pub async fn process(&mut self) -> Result<(), ProcessorError> {
        tracing::info!("Starting processing with manifest: {}", self.manifest.name);

        tracing::info!("Processing model files...");
        let mut model_sequence = self.model_sequence();
        for step in model_sequence.drain(..) {
            self.process_model_step(step).await?;
        }

        // Clone the vocabulary for the instance processor before consuming it to write to disk
        let vocabulary = self.vocabulary_manager.get_vocabulary();

        // Generate and save vocabulary
        self.vocabulary_manager
            .save_vocabulary(&self.output_path)
            .await?;

        self.instance_manager.set_vocabulary(vocabulary);

        tracing::info!("Processing instance files...");
        let instance_sequence = self.instance_sequence();
        for step in instance_sequence {
            self.process_instance_step(&step).await?;
        }

        // Save instance data
        self.instance_manager
            .save_instances(&self.output_path)
            .await?;

        tracing::info!("Processing completed successfully");
        Ok(())
    }

    async fn process_model_step(&mut self, step: ImportStep) -> Result<(), ProcessorError> {
        tracing::info!(
            "Processing model step: {} (types: {:?})",
            step.path,
            step.types
        );
        let model_path = self.resolve_path(&self.manifest.model.path);

        let step_name = step.path.clone();

        if contains_variant!(step.types, StepType::ModelStep(_)) {
            tracing::debug!("Processing as base vocabulary data");
            self.vocabulary_manager
                .process_vocabulary(step, model_path.to_str().unwrap())
                .await?;
        }

        tracing::debug!("At end of step: {:?}", step_name);
        tracing::debug!(
            "Current vocabulary: {:#?}",
            self.vocabulary_manager.processor.vocabulary
        );

        Ok(())
    }

    async fn process_instance_step(
        &mut self,
        step: &crate::manifest::ImportStep,
    ) -> Result<(), ProcessorError> {
        tracing::info!(
            "Processing instance step: {} (types: {:?})",
            step.path,
            step.types
        );

        let instance_path = self.resolve_path(&self.manifest.instances.path);

        // Find the instance step type
        let instance_step = step
            .types
            .iter()
            .find_map(|t| match t {
                StepType::InstanceStep(step_type) => Some(step_type),
                _ => None,
            })
            .ok_or_else(|| {
                ProcessorError::Processing("No valid instance step type found".into())
            })?;

        // Process based on step type
        match instance_step {
            InstanceStep::BasicInstanceStep => {
                tracing::debug!("Processing as basic instance data");
                self.instance_manager
                    .process_simple_instance(step, instance_path.to_str().unwrap())
                    .await?;
            }
            InstanceStep::SubClassInstanceStep => {
                tracing::debug!("Processing as subclass instance data");
                self.instance_manager
                    .process_subclass_instance(step, instance_path.to_str().unwrap())
                    .await?;
            }
            InstanceStep::PropertiesInstanceStep => {
                tracing::debug!("Processing as properties instance data");
                self.instance_manager
                    .process_properties_instance(step, instance_path.to_str().unwrap())
                    .await?;
            }
        }

        Ok(())
    }
}
