use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use lance::dataset::optimize::{compact_files, CompactionOptions};
use seaweed_worker_core::config_form::{form, int_or, int_value, number_field, string_value};
use seaweed_worker_core::pb::{
    ConfigValue, DetectionComplete, DetectionProposals, ExecuteJobRequest, JobCompleted,
    JobProgressUpdate, JobProposal, JobResult, JobTypeCapability, JobTypeDescriptor,
    RunDetectionRequest, WorkerObservations,
};
use seaweed_worker_core::{DetectionSender, ExecutionSender, JobHandler};
use tracing::warn;

use crate::catalog::{parse_id, NamespaceClient};
use crate::dataset;
use crate::jobs::{clamp, observation, string_list, table_id, FORMAT};

pub const JOB_TYPE: &str = "lance_compact";

const DEFAULT_TARGET_ROWS: i64 = 1_048_576;
const DEFAULT_MIN_FRAGMENTS: i64 = 8;

// The ranges the descriptor's form offers. These are also the values that stay
// meaningful: a table needs two fragments before merging them means anything,
// and a fragment target below a thousand rows defeats the purpose of the job.
// Both are cast to usize, where a negative would arrive as an enormous number.
const TARGET_ROWS_FLOOR: i64 = 1024;
const TARGET_ROWS_CEILING: i64 = 16_777_216;
const MIN_FRAGMENTS_FLOOR: i64 = 2;
const MIN_FRAGMENTS_CEILING: i64 = 4096;

/// Lance writes one fragment per write batch, so a table fed by small appends
/// accumulates small files the same way an Iceberg table does.
pub struct CompactHandler {
    namespace_url: String,
    fallback: dataset::FallbackOptions,
    metrics: Option<crate::metrics::LanceMetrics>,
}

impl CompactHandler {
    pub fn new(namespace_url: String) -> Self {
        Self {
            namespace_url,
            fallback: dataset::FallbackOptions::new(),
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Option<crate::metrics::LanceMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Storage options to use where the namespace vends none.
    pub fn with_fallback(mut self, fallback: dataset::FallbackOptions) -> Self {
        self.fallback = fallback;
        self
    }

    fn client(&self) -> NamespaceClient {
        NamespaceClient::new(self.namespace_url.clone())
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
        defaults.insert(
            "target_rows_per_fragment".to_string(),
            int_value(DEFAULT_TARGET_ROWS),
        );
        defaults.insert(
            "min_fragments".to_string(),
            int_value(DEFAULT_MIN_FRAGMENTS),
        );

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

    /// Propose a job for every table with more fragments than the operator is
    /// willing to leave alone. Opening a dataset reads its manifest, not its
    /// data, so this stays cheap across a catalog.
    async fn detect(
        &self,
        request: &RunDetectionRequest,
        sender: &dyn DetectionSender,
    ) -> Result<()> {
        let min_fragments = clamp(
            int_or(
                &request.worker_config_values,
                "min_fragments",
                DEFAULT_MIN_FRAGMENTS,
            ),
            MIN_FRAGMENTS_FLOOR,
            MIN_FRAGMENTS_CEILING,
        ) as usize;
        let client = self.client();
        let tables = client.list_all_tables().await?;

        let mut proposals = Vec::new();
        let mut observations = Vec::new();
        for encoded in &tables {
            let id = parse_id(encoded);
            let table = match dataset::open(&client, &id, &self.fallback).await {
                Ok(table) => table,
                Err(err) => {
                    // A table that cannot be opened is the next run's problem,
                    // not a reason to abandon the whole sweep.
                    if let Some(counters) = &self.metrics {
                        counters.worker.object_skipped(JOB_TYPE, "open");
                    }
                    warn!("skipping {encoded}: {err:#}");
                    continue;
                }
            };
            // One unreadable table must not end the sweep: the tables already
            // read would lose their proposals, and admin would get no
            // completion for this request at all.
            if let Some(counters) = &self.metrics {
                counters.worker.object_seen(JOB_TYPE);
            }
            let stats = match table.stats().await {
                Ok(stats) => stats,
                Err(err) => {
                    if let Some(counters) = &self.metrics {
                        counters.worker.object_skipped(JOB_TYPE, "stats");
                    }
                    warn!("skipping {encoded}: reading its stats failed: {err:#}");
                    continue;
                }
            };
            // Logged because "detection proposed nothing" is otherwise
            // indistinguishable from a table the worker could not read.
            tracing::info!(
                "compaction detection: {encoded} has {} fragments, threshold {min_fragments}",
                stats.fragments
            );

            let mut attributes: HashMap<String, ConfigValue> = HashMap::new();
            attributes.insert("fragments".to_string(), int_value(stats.fragments as i64));
            attributes.insert("version".to_string(), int_value(stats.version as i64));
            attributes.insert(
                "versions".to_string(),
                int_value(stats.total_versions as i64),
            );
            attributes.insert("rows".to_string(), int_value(stats.rows as i64));
            if let Some(schema) = stats.schema.clone() {
                attributes.insert("schema".to_string(), string_value(schema));
            }
            observations.push(observation(&id, FORMAT, attributes));

            if stats.fragments < min_fragments {
                continue;
            }
            let mut parameters: HashMap<String, ConfigValue> = HashMap::new();
            parameters.insert("table_id".to_string(), string_list(&id));
            proposals.push(JobProposal {
                proposal_id: format!("{JOB_TYPE}:{encoded}"),
                dedupe_key: format!("{JOB_TYPE}:{encoded}"),
                job_type: JOB_TYPE.to_string(),
                summary: format!("Compact {encoded} ({} fragments)", stats.fragments),
                detail: format!(
                    "{} fragments at version {}, above the {min_fragments} the policy allows",
                    stats.fragments, stats.version
                ),
                parameters,
                ..Default::default()
            });
        }

        if !observations.is_empty() {
            sender.send_observations(WorkerObservations {
                job_type: JOB_TYPE.to_string(),
                observations,
            })?;
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
        let target_rows = clamp(
            int_or(
                &request.worker_config_values,
                "target_rows_per_fragment",
                DEFAULT_TARGET_ROWS,
            ),
            TARGET_ROWS_FLOOR,
            TARGET_ROWS_CEILING,
        ) as usize;

        let client = self.client();
        // Re-resolve rather than trusting the location detection saw: the table
        // may have been repointed, and the vended credentials have expired.
        let mut table = dataset::open(&client, &id, &self.fallback).await?;
        let before = table.stats().await?;

        sender.send_progress(JobProgressUpdate {
            request_id: request.request_id.clone(),
            job_id: job.job_id.clone(),
            job_type: JOB_TYPE.to_string(),
            progress_percent: 10.0,
            stage: format!("compacting {} fragments", before.fragments),
            ..Default::default()
        })?;

        let options = CompactionOptions {
            target_rows_per_fragment: target_rows,
            ..Default::default()
        };
        let metrics = compact_files(&mut table.dataset, options, None)
            .await
            .with_context(|| format!("compact {}", table.location))?;

        if let Some(counters) = &self.metrics {
            counters
                .fragments_removed
                .inc_by(metrics.fragments_removed as u64);
        }

        let after = table.stats().await?;
        let mut output: HashMap<String, ConfigValue> = HashMap::new();
        output.insert(
            "fragments_removed".to_string(),
            int_value(metrics.fragments_removed as i64),
        );
        output.insert(
            "fragments_added".to_string(),
            int_value(metrics.fragments_added as i64),
        );
        output.insert(
            "files_removed".to_string(),
            int_value(metrics.files_removed as i64),
        );

        sender.send_completed(JobCompleted {
            request_id: request.request_id.clone(),
            job_id: job.job_id.clone(),
            job_type: JOB_TYPE.to_string(),
            success: true,
            result: Some(JobResult {
                output_values: output,
                summary: format!("{} fragments became {}", before.fragments, after.fragments),
                ..Default::default()
            }),
            ..Default::default()
        })?;
        Ok(())
    }
}
