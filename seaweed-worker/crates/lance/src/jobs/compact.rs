use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use seaweed_worker_core::config_form::{form, int_or, int_value, number_field};
use seaweed_worker_core::pb::{
    ConfigValue, ExecuteJobRequest, JobTypeCapability, JobTypeDescriptor, RunDetectionRequest,
};
use seaweed_worker_core::{DetectionSender, ExecutionSender, JobHandler};

pub const JOB_TYPE: &str = "lance_compact";

/// Lance writes one fragment per write batch, so a table fed by small appends
/// accumulates small files the same way an Iceberg table does.
pub struct CompactHandler {
    #[allow(dead_code)]
    namespace_url: String,
}

impl CompactHandler {
    pub fn new(namespace_url: String) -> Self {
        Self { namespace_url }
    }
}

#[async_trait]
impl JobHandler for CompactHandler {
    fn capability(&self) -> JobTypeCapability {
        JobTypeCapability {
            job_type: JOB_TYPE.to_string(),
            can_detect: true,
            can_execute: true,
            max_detection_concurrency: 1,
            max_execution_concurrency: 1,
            display_name: "Lance Compaction".to_string(),
            description: "Merge small Lance fragments into fewer, larger ones".to_string(),
            weight: 20,
        }
    }

    fn descriptor(&self) -> JobTypeDescriptor {
        let mut defaults: HashMap<String, ConfigValue> = HashMap::new();
        defaults.insert("target_rows_per_fragment".to_string(), int_value(1_048_576));
        defaults.insert("min_fragments".to_string(), int_value(8));

        JobTypeDescriptor {
            job_type: JOB_TYPE.to_string(),
            display_name: "Lance Compaction".to_string(),
            description: "Compact fragments of Lance tables".to_string(),
            icon: "fas fa-compress".to_string(),
            descriptor_version: 1,
            worker_config_form: Some(form(
                "lance-compact-worker",
                "Compaction",
                vec![
                    number_field(
                        "target_rows_per_fragment",
                        "Target rows per fragment",
                        "Rows to aim for when rewriting fragments",
                        1024,
                        16_777_216,
                    ),
                    number_field(
                        "min_fragments",
                        "Minimum fragments",
                        "Leave a table alone until it has at least this many fragments",
                        2,
                        4096,
                    ),
                ],
                defaults.clone(),
            )),
            worker_default_values: defaults,
            ..Default::default()
        }
    }

    async fn detect(&self, request: &RunDetectionRequest, _sender: &dyn DetectionSender) -> Result<()> {
        let _min_fragments = int_or(&request.worker_config_values, "min_fragments", 8);
        Err(anyhow!(
            "lance compaction needs the lance crate to count fragments; not implemented yet"
        ))
    }

    async fn execute(&self, _request: &ExecuteJobRequest, _sender: &dyn ExecutionSender) -> Result<()> {
        Err(anyhow!(
            "lance compaction needs the lance crate to rewrite fragments; not implemented yet"
        ))
    }
}
