use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use seaweed_worker_core::config_form::{form, int_value, number_field};
use seaweed_worker_core::pb::{
    ConfigValue, ExecuteJobRequest, JobTypeCapability, JobTypeDescriptor, RunDetectionRequest,
};
use seaweed_worker_core::{DetectionSender, ExecutionSender, JobHandler};

pub const JOB_TYPE: &str = "lance_optimize_indices";

/// Rows written after an index was built are not covered by it, so a vector
/// search quietly misses them. This is the job with no Iceberg equivalent, and
/// the reason a stale Lance table is a correctness problem rather than a slow
/// one.
pub struct OptimizeIndicesHandler {
    #[allow(dead_code)]
    namespace_url: String,
}

impl OptimizeIndicesHandler {
    pub fn new(namespace_url: String) -> Self {
        Self { namespace_url }
    }
}

#[async_trait]
impl JobHandler for OptimizeIndicesHandler {
    fn capability(&self) -> JobTypeCapability {
        JobTypeCapability {
            job_type: JOB_TYPE.to_string(),
            can_detect: true,
            can_execute: true,
            max_detection_concurrency: 1,
            max_execution_concurrency: 1,
            display_name: "Lance Index Optimization".to_string(),
            description: "Extend indices to cover rows written since they were built".to_string(),
            weight: 30,
        }
    }

    fn descriptor(&self) -> JobTypeDescriptor {
        let mut defaults: HashMap<String, ConfigValue> = HashMap::new();
        defaults.insert("max_unindexed_rows".to_string(), int_value(100_000));

        JobTypeDescriptor {
            job_type: JOB_TYPE.to_string(),
            display_name: "Lance Index Optimization".to_string(),
            description: "Keep vector and scalar indices covering the whole table".to_string(),
            icon: "fas fa-magnifying-glass-chart".to_string(),
            descriptor_version: 1,
            worker_config_form: Some(form(
                "lance-indices-worker",
                "Index optimization",
                vec![number_field(
                    "max_unindexed_rows",
                    "Unindexed row budget",
                    "Reindex once a table has more rows than this outside its indices",
                    1_000,
                    100_000_000,
                )],
                defaults.clone(),
            )),
            worker_default_values: defaults,
            ..Default::default()
        }
    }

    async fn detect(&self, _request: &RunDetectionRequest, _sender: &dyn DetectionSender) -> Result<()> {
        Err(anyhow!(
            "index optimization needs the lance crate to read index statistics; not implemented yet"
        ))
    }

    async fn execute(&self, _request: &ExecuteJobRequest, _sender: &dyn ExecutionSender) -> Result<()> {
        Err(anyhow!(
            "index optimization needs the lance crate to rebuild indices; not implemented yet"
        ))
    }
}
