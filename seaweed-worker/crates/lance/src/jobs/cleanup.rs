use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use seaweed_worker_core::config_form::{form, int_value, number_field};
use seaweed_worker_core::pb::{
    ConfigValue, ExecuteJobRequest, JobTypeCapability, JobTypeDescriptor, RunDetectionRequest,
};
use seaweed_worker_core::{DetectionSender, ExecutionSender, JobHandler};

pub const JOB_TYPE: &str = "lance_cleanup_versions";

/// Lance keeps every version until something removes it. Lance can also do this
/// itself through auto-cleanup, so this job is for deployments that would rather
/// the cluster owned the policy than each writer.
pub struct CleanupVersionsHandler {
    #[allow(dead_code)]
    namespace_url: String,
}

impl CleanupVersionsHandler {
    pub fn new(namespace_url: String) -> Self {
        Self { namespace_url }
    }
}

#[async_trait]
impl JobHandler for CleanupVersionsHandler {
    fn capability(&self) -> JobTypeCapability {
        JobTypeCapability {
            job_type: JOB_TYPE.to_string(),
            can_detect: true,
            can_execute: true,
            max_detection_concurrency: 1,
            max_execution_concurrency: 1,
            display_name: "Lance Version Cleanup".to_string(),
            description: "Remove old Lance versions and the files only they referenced".to_string(),
            weight: 10,
        }
    }

    fn descriptor(&self) -> JobTypeDescriptor {
        let mut defaults: HashMap<String, ConfigValue> = HashMap::new();
        defaults.insert("retain_hours".to_string(), int_value(168));
        defaults.insert("min_versions_to_keep".to_string(), int_value(5));

        JobTypeDescriptor {
            job_type: JOB_TYPE.to_string(),
            display_name: "Lance Version Cleanup".to_string(),
            description: "Age out Lance versions the table no longer needs".to_string(),
            icon: "fas fa-broom".to_string(),
            descriptor_version: 1,
            worker_config_form: Some(form(
                "lance-cleanup-worker",
                "Version cleanup",
                vec![
                    number_field(
                        "retain_hours",
                        "Retain versions for (hours)",
                        "Versions younger than this are always kept",
                        1,
                        8760,
                    ),
                    number_field(
                        "min_versions_to_keep",
                        "Minimum versions",
                        "Never leave a table with fewer versions than this, whatever their age",
                        1,
                        1000,
                    ),
                ],
                defaults.clone(),
            )),
            worker_default_values: defaults,
            ..Default::default()
        }
    }

    async fn detect(&self, _request: &RunDetectionRequest, _sender: &dyn DetectionSender) -> Result<()> {
        Err(anyhow!(
            "version cleanup needs the lance crate to resolve what a version references; not implemented yet"
        ))
    }

    async fn execute(&self, _request: &ExecuteJobRequest, _sender: &dyn ExecutionSender) -> Result<()> {
        Err(anyhow!(
            "version cleanup needs the lance crate to remove versions safely; not implemented yet"
        ))
    }
}
