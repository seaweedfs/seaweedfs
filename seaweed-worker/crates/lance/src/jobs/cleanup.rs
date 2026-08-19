use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use lance::dataset::cleanup::{cleanup_old_versions, CleanupPolicy};
use seaweed_worker_core::config_form::{form, int_or, int_value, number_field};
use seaweed_worker_core::pb::{
    ConfigValue, DetectionComplete, DetectionProposals, ExecuteJobRequest, JobCompleted,
    JobProgressUpdate, JobProposal, JobResult, JobTypeCapability, JobTypeDescriptor,
    RunDetectionRequest,
};
use seaweed_worker_core::{DetectionSender, ExecutionSender, JobHandler};
use tracing::warn;

use crate::catalog::{parse_id, NamespaceClient};
use crate::dataset;
use crate::jobs::{string_list, table_id};

pub const JOB_TYPE: &str = "lance_cleanup_versions";

const DEFAULT_RETAIN_HOURS: i64 = 168;
const DEFAULT_MIN_VERSIONS: i64 = 5;

/// Lance keeps every version until something removes it. Lance can also do this
/// itself through auto-cleanup, so this job is for deployments that would rather
/// the cluster owned the policy than each writer.
pub struct CleanupVersionsHandler {
    namespace_url: String,
    fallback: dataset::FallbackOptions,
}

impl CleanupVersionsHandler {
    pub fn new(namespace_url: String) -> Self {
        Self {
            namespace_url,
            fallback: dataset::FallbackOptions::new(),
        }
    }

    pub fn with_fallback(mut self, fallback: dataset::FallbackOptions) -> Self {
        self.fallback = fallback;
        self
    }

    fn client(&self) -> NamespaceClient {
        NamespaceClient::new(self.namespace_url.clone())
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
        defaults.insert("retain_hours".to_string(), int_value(DEFAULT_RETAIN_HOURS));
        defaults.insert(
            "min_versions_to_keep".to_string(),
            int_value(DEFAULT_MIN_VERSIONS),
        );

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

    async fn detect(
        &self,
        request: &RunDetectionRequest,
        sender: &dyn DetectionSender,
    ) -> Result<()> {
        let min_versions = int_or(
            &request.worker_config_values,
            "min_versions_to_keep",
            DEFAULT_MIN_VERSIONS,
        ) as usize;
        let client = self.client();
        let tables = client.list_all_tables().await?;

        let mut proposals = Vec::new();
        for encoded in &tables {
            let id = parse_id(encoded);
            let table = match dataset::open(&client, &id, &self.fallback).await {
                Ok(table) => table,
                Err(err) => {
                    warn!("skipping {encoded}: {err:#}");
                    continue;
                }
            };
            let stats = table.stats().await?;
            // Age is decided at execution against the retention window; a table
            // at or under the floor cannot lose a version whatever its age, so
            // proposing one would only produce a job with nothing to do.
            if stats.total_versions <= min_versions {
                continue;
            }
            let mut parameters: HashMap<String, ConfigValue> = HashMap::new();
            parameters.insert("table_id".to_string(), string_list(&id));
            proposals.push(JobProposal {
                proposal_id: format!("{JOB_TYPE}:{encoded}"),
                dedupe_key: format!("{JOB_TYPE}:{encoded}"),
                job_type: JOB_TYPE.to_string(),
                summary: format!("Clean up {encoded} ({} versions)", stats.total_versions),
                detail: format!(
                    "{} versions retained, above the {min_versions} floor; \
                     those outside the retention window can go",
                    stats.total_versions
                ),
                parameters,
                ..Default::default()
            });
        }

        let total = proposals.len() as i32;
        sender.send_proposals(DetectionProposals {
            request_id: request.request_id.clone(),
            job_type: JOB_TYPE.to_string(),
            proposals,
            has_more: false,
        })?;
        sender.send_complete(DetectionComplete {
            request_id: request.request_id.clone(),
            job_type: JOB_TYPE.to_string(),
            success: true,
            error_message: String::new(),
            total_proposals: total,
        })?;
        Ok(())
    }

    async fn execute(
        &self,
        request: &ExecuteJobRequest,
        sender: &dyn ExecutionSender,
    ) -> Result<()> {
        let job = request
            .job
            .as_ref()
            .ok_or_else(|| anyhow!("execute request carried no job"))?;
        let id = table_id(&job.parameters)
            .ok_or_else(|| anyhow!("job {} carried no table_id", job.job_id))?;
        let retain_hours = int_or(
            &request.worker_config_values,
            "retain_hours",
            DEFAULT_RETAIN_HOURS,
        );

        let client = self.client();
        let table = dataset::open(&client, &id, &self.fallback).await?;
        let before = table.stats().await?;

        sender.send_progress(JobProgressUpdate {
            request_id: request.request_id.clone(),
            job_id: job.job_id.clone(),
            job_type: JOB_TYPE.to_string(),
            progress_percent: 10.0,
            stage: format!("cleaning up {} versions", before.total_versions),
            ..Default::default()
        })?;

        let policy = CleanupPolicy {
            before_timestamp: Some(Utc::now() - Duration::hours(retain_hours)),
            // Files this dataset cannot account for are left alone: they may
            // belong to a writer that has not committed yet, and deleting them
            // would corrupt a commit in flight.
            delete_unverified: false,
            // A tagged version is pinned on purpose, so refuse rather than
            // silently dropping what someone named.
            error_if_tagged_old_versions: true,
            ..Default::default()
        };
        let stats = cleanup_old_versions(&table.dataset, policy)
            .await
            .with_context(|| format!("clean up versions of {}", table.location))?;

        let mut output: HashMap<String, ConfigValue> = HashMap::new();
        output.insert("old_versions_removed".to_string(), int_value(stats.old_versions as i64));
        output.insert("bytes_removed".to_string(), int_value(stats.bytes_removed as i64));
        output.insert(
            "data_files_removed".to_string(),
            int_value(stats.data_files_removed as i64),
        );

        sender.send_completed(JobCompleted {
            request_id: request.request_id.clone(),
            job_id: job.job_id.clone(),
            job_type: JOB_TYPE.to_string(),
            success: true,
            result: Some(JobResult {
                output_values: output,
                summary: format!(
                    "removed {} versions and {} bytes",
                    stats.old_versions, stats.bytes_removed
                ),
                ..Default::default()
            }),
            ..Default::default()
        })?;
        Ok(())
    }
}
