use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use lance::index::DatasetIndexExt;
use lance_index::optimize::OptimizeOptions;
use seaweed_worker_core::config_form::{form, int_or, int_value, number_field};
use seaweed_worker_core::pb::{
    ConfigValue, DetectionComplete, DetectionProposals, ExecuteJobRequest, JobCompleted,
    JobProgressUpdate, JobProposal, JobResult, JobTypeCapability, JobTypeDescriptor,
    RunDetectionRequest,
};
use seaweed_worker_core::{DetectionSender, ExecutionSender, JobHandler};
use tracing::warn;

use crate::catalog::{parse_id, NamespaceClient};
use crate::dataset::{self, OpenTable};
use crate::jobs::{string_list, table_id};

pub const JOB_TYPE: &str = "lance_optimize_indices";

const DEFAULT_MAX_UNINDEXED_ROWS: i64 = 100_000;

/// Rows written after an index was built are not covered by it, so a vector
/// search quietly misses them. This is the job with no Iceberg equivalent, and
/// the reason a neglected Lance table is a correctness problem rather than a
/// slow one.
pub struct OptimizeIndicesHandler {
    namespace_url: String,
    fallback: dataset::FallbackOptions,
}

impl OptimizeIndicesHandler {
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

/// Rows no index covers, summed across a table's indices. A table with no
/// indices at all has nothing to optimize, which is different from a table whose
/// indices have fallen behind.
async fn unindexed_rows(table: &OpenTable) -> Result<Option<u64>> {
    let indices = table.dataset.load_indices().await?;
    if indices.is_empty() {
        return Ok(None);
    }

    let mut worst = 0u64;
    let mut names: Vec<String> = indices.iter().map(|index| index.name.clone()).collect();
    names.sort();
    names.dedup();
    for name in names {
        let raw = table.dataset.index_statistics(&name).await?;
        let stats: serde_json::Value = serde_json::from_str(&raw)
            .with_context(|| format!("parse index statistics for {name}"))?;
        let unindexed = stats
            .get("num_unindexed_rows")
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        worst = worst.max(unindexed);
    }
    Ok(Some(worst))
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
        defaults.insert(
            "max_unindexed_rows".to_string(),
            int_value(DEFAULT_MAX_UNINDEXED_ROWS),
        );

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

    async fn detect(
        &self,
        request: &RunDetectionRequest,
        sender: &dyn DetectionSender,
    ) -> Result<()> {
        let budget = int_or(
            &request.worker_config_values,
            "max_unindexed_rows",
            DEFAULT_MAX_UNINDEXED_ROWS,
        ) as u64;
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
            let Some(unindexed) = unindexed_rows(&table).await? else {
                continue;
            };
            if unindexed <= budget {
                continue;
            }
            let mut parameters: HashMap<String, ConfigValue> = HashMap::new();
            parameters.insert("table_id".to_string(), string_list(&id));
            proposals.push(JobProposal {
                proposal_id: format!("{JOB_TYPE}:{encoded}"),
                dedupe_key: format!("{JOB_TYPE}:{encoded}"),
                job_type: JOB_TYPE.to_string(),
                summary: format!("Reindex {encoded} ({unindexed} rows uncovered)"),
                detail: format!(
                    "{unindexed} rows sit outside the indices, above the {budget} the policy allows; \
                     a search of this table misses them"
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

        let client = self.client();
        let mut table = dataset::open(&client, &id, &self.fallback).await?;
        let before = unindexed_rows(&table).await?.unwrap_or(0);

        sender.send_progress(JobProgressUpdate {
            request_id: request.request_id.clone(),
            job_id: job.job_id.clone(),
            job_type: JOB_TYPE.to_string(),
            progress_percent: 10.0,
            stage: format!("indexing {before} uncovered rows"),
            ..Default::default()
        })?;

        // Merging every delta keeps read latency from drifting as the table is
        // reindexed again and again; leaving them unmerged is how an index ends
        // up fast to write and slow to search.
        table
            .dataset
            .optimize_indices(&OptimizeOptions::default())
            .await
            .with_context(|| format!("optimize indices of {}", table.location))?;

        let after = unindexed_rows(&table).await?.unwrap_or(0);
        let mut output: HashMap<String, ConfigValue> = HashMap::new();
        output.insert(
            "unindexed_rows_before".to_string(),
            int_value(before as i64),
        );
        output.insert("unindexed_rows_after".to_string(), int_value(after as i64));

        sender.send_completed(JobCompleted {
            request_id: request.request_id.clone(),
            job_id: job.job_id.clone(),
            job_type: JOB_TYPE.to_string(),
            success: true,
            result: Some(JobResult {
                output_values: output,
                summary: format!("{before} uncovered rows became {after}"),
                ..Default::default()
            }),
            ..Default::default()
        })?;
        Ok(())
    }
}
