use crate::error::ProcessorError;
use crate::instance::InstanceManager;
use crate::manifest::{InstanceStep, Manifest, StepType};
use crate::vocabulary::VocabularyManager;
use crate::{contains_variant, ImportStep};
use std::path::PathBuf;
use std::sync::Arc;

pub struct ProcessorBuilder {
    manifest: Manifest,
    base_path: Option<PathBuf>,
    output_path: Option<PathBuf>,
    is_strict: bool,
    export_vocab_meta: bool,
    vocab_meta_path: Option<PathBuf>,
}

impl ProcessorBuilder {
    pub fn from_manifest(manifest: Manifest) -> Self {
        Self {
            manifest,
            base_path: None,
            output_path: None,
            is_strict: false,
            export_vocab_meta: false,
            vocab_meta_path: None,
        }
    }

    pub fn with_base_path<P: Into<PathBuf>>(mut self, base_path: P) -> Self {
        self.base_path = Some(base_path.into());
        self
    }

    pub fn with_output_path<P: Into<PathBuf>>(mut self, output_path: P) -> Self {
        self.output_path = Some(output_path.into());
        self
    }

    pub fn with_strict(mut self, is_strict: bool) -> Self {
        self.is_strict = is_strict;
        self
    }

    pub fn with_export_vocab_meta(mut self, export_vocab_meta: bool) -> Self {
        self.export_vocab_meta = export_vocab_meta;
        self
    }

    pub fn with_vocab_meta_path<P: Into<PathBuf>>(mut self, vocab_meta_path: P) -> Self {
        self.vocab_meta_path = Some(vocab_meta_path.into());
        self
    }

    pub fn build(self) -> Result<Processor, ProcessorError> {
        let base_path = match self.base_path {
            Some(path) => path,
            None => {
                tracing::warn!("Base path not set, using current directory");
                std::env::current_dir()?
            }
        };
        let output_path = match self.output_path {
            Some(path) => path,
            None => {
                tracing::warn!("Output path not set, using current directory");
                std::env::current_dir()?
            }
        };

        Processor::with_base_path(
            self.manifest,
            base_path,
            self.is_strict,
            output_path,
            self.export_vocab_meta,
            self.vocab_meta_path,
        )
    }
}

pub struct Processor {
    manifest: Arc<Manifest>,
    vocabulary_manager: VocabularyManager,
    instance_manager: InstanceManager,
    base_path: PathBuf,
    output_path: PathBuf,
    export_vocab_meta: bool,
}

impl Processor {
    pub fn with_base_path<P: Into<PathBuf>>(
        manifest: Manifest,
        base_path: P,
        is_strict: bool,
        output_path: P,
        export_vocab_meta: bool,
        vocab_meta_path: Option<PathBuf>,
    ) -> Result<Self, ProcessorError> {
        let base_path = base_path.into();
        let output_path = output_path.into();
        let manifest = Arc::new(manifest);
        tracing::info!("Creating processor with base path: {:?}", base_path);
        Ok(Self {
            vocabulary_manager: VocabularyManager::new(
                Arc::clone(&manifest),
                is_strict,
                vocab_meta_path,
            )?,
            instance_manager: InstanceManager::new(Arc::clone(&manifest), is_strict),
            base_path,
            output_path,
            manifest,
            export_vocab_meta,
        })
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
        let vocabulary = self.vocabulary_manager.processor.take_vocabulary();

        // Generate and save vocabulary
        // self.vocabulary_manager
        //     .save_vocabulary(&self.output_path)
        //     .await?;

        self.instance_manager.set_vocabulary(vocabulary);

        tracing::info!("Processing instance files...");
        let mut instance_sequence = self.instance_sequence();

        // Order sequence so that any ImportStep with InstanceStep::PicklistStep is processed first
        // and if we do reorder the instance_sequence, we should warn the user
        let mut did_reorder_instance_sequence = false;
        instance_sequence.sort_by(|a, b| {
            let a_is_picklist = a
                .types
                .iter()
                .any(|t| matches!(t, StepType::InstanceStep(InstanceStep::PicklistStep)));
            let b_is_picklist = b
                .types
                .iter()
                .any(|t| matches!(t, StepType::InstanceStep(InstanceStep::PicklistStep)));

            if a_is_picklist && !b_is_picklist {
                did_reorder_instance_sequence = true;
                std::cmp::Ordering::Less
            } else if !a_is_picklist && b_is_picklist {
                did_reorder_instance_sequence = true;
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });
        if did_reorder_instance_sequence {
            tracing::warn!("Reordered instance sequence to process PicklistStep(s) first");
        };

        for step in instance_sequence {
            self.process_instance_step(&step).await?;
        }

        // Save instance data; this takes place before save_vocabulary, which will take the vocabulary in memory
        self.instance_manager
            .save_instances(&self.output_path)
            .await?;

        let vocabulary = self.instance_manager.take_vocabulary();

        if self.export_vocab_meta {
            self.vocabulary_manager
                .save_vocabulary_meta(&vocabulary, &self.output_path)
                .await?;
        };

        self.vocabulary_manager
            .save_vocabulary(vocabulary, &self.output_path)
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
        // tracing::debug!(
        //     "Current vocabulary: {:#?}",
        //     self.vocabulary_manager.processor.vocabulary
        // );

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
            InstanceStep::BasicInstanceStep | InstanceStep::PicklistStep => {
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
