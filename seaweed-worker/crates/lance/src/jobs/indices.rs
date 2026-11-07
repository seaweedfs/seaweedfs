use std::collections::HashMap;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use lance::index::DatasetIndexExt;
use lance_index::optimize::OptimizeOptions;
use seaweed_worker_core::config_form::{form, int_or, int_value, number_field};
use seaweed_worker_core::pb::{
    AdminRuntimeDefaults, ConfigValue, DetectionComplete, DetectionProposals, ExecuteJobRequest,
    JobCompleted, JobProgressUpdate, JobProposal, JobResult, JobTypeCapability, JobTypeDescriptor,
    RunDetectionRequest,
};
use seaweed_worker_core::{DetectionSender, ExecutionSender, JobHandler};
use tracing::warn;

use crate::catalog::{NamespaceClient, SigningKey, parse_id};
use crate::dataset::{self, OpenTable};
use crate::jobs::vector_index::is_vector_table;
use crate::jobs::{clamp, string_list, table_id};
use crate::vector_index;

pub const JOB_TYPE: &str = "lance_optimize_indices";
/// The vector column the S3 Vectors API writes.
const VECTOR_COLUMN: &str = "vector";

const DEFAULT_MAX_UNINDEXED_ROWS: i64 = 100_000;

// Zero is a real setting - reindex as soon as any row is uncovered - so the
// floor is what stays meaningful rather than what the form offers. The ceiling
// is the form's, and the point of both is that this is cast to u64: a negative
// would arrive as an enormous budget and mean "never reindex".
const MAX_UNINDEXED_FLOOR: i64 = 0;
const MAX_UNINDEXED_CEILING: i64 = 100_000_000;

/// Rows written after an index was built are not covered by it, so a vector
/// search quietly misses them. This is the job with no Iceberg equivalent, and
/// the reason a neglected Lance table is a correctness problem rather than a
/// slow one.
pub struct OptimizeIndicesHandler {
    namespace_url: String,
    storage: dataset::Storage,
    signing_key: Option<SigningKey>,
    metrics: Option<crate::metrics::LanceMetrics>,
}

impl OptimizeIndicesHandler {
    pub fn new(namespace_url: String) -> Self {
        Self {
            namespace_url,
            storage: dataset::Storage::default(),
            signing_key: None,
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Option<crate::metrics::LanceMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_storage(mut self, storage: dataset::Storage) -> Self {
        self.storage = storage;
        self
    }

    pub fn with_signing_key(mut self, signing_key: Option<SigningKey>) -> Self {
        self.signing_key = signing_key;
        self
    }

    fn client(&self) -> NamespaceClient {
        NamespaceClient::new(self.namespace_url.clone()).with_signing_key(self.signing_key.as_ref())
    }
}

/// Rows the table's indices cover and rows they miss. A vector table's indices
/// are taken together, since a sharded build gives one name per shard; any
/// other table reports the index that misses the most. A table with no
/// indices at all has nothing to optimize, which is different from a table
/// whose indices have fallen behind.
struct IndexRows {
    indexed: u64,
    unindexed: u64,
}

async fn index_rows(table: &OpenTable, vector_table: bool) -> Result<Option<IndexRows>> {
    if vector_table {
        return Ok(vector_index::coverage(&table.dataset, VECTOR_COLUMN)
            .await?
            .map(|cover| IndexRows {
                indexed: cover.indexed_rows,
                unindexed: cover.unindexed_rows,
            }));
    }
    let indices = table.dataset.load_indices().await?;
    if indices.is_empty() {
        return Ok(None);
    }

    let mut worst = IndexRows {
        indexed: 0,
        unindexed: 0,
    };
    let mut names: Vec<String> = indices.iter().map(|index| index.name.clone()).collect();
    names.sort();
    names.dedup();
    for name in names {
        let raw = table.dataset.index_statistics(&name).await?;
        let stats: serde_json::Value = serde_json::from_str(&raw)
            .with_context(|| format!("parse index statistics for {name}"))?;
        let count = |key: &str| stats.get(key).and_then(|value| value.as_u64()).unwrap_or(0);
        if count("num_unindexed_rows") >= worst.unindexed {
            worst = IndexRows {
                indexed: count("num_indexed_rows"),
                unindexed: count("num_unindexed_rows"),
            };
        }
    }
    Ok(Some(worst))
}

/// A vector index also reindexes once a tenth of it is uncovered, so a small
/// index does not wait for the absolute budget.
fn effective_budget(budget: u64, rows: &IndexRows, vector_table: bool) -> u64 {
    if vector_table {
        budget.min(rows.indexed / 10)
    } else {
        budget
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
            // A retrain is a whole build, so it gets a build's time.
            admin_runtime_defaults: Some(AdminRuntimeDefaults {
                job_type_max_runtime_seconds: super::vector_index::EXECUTION_TIMEOUT_SECONDS,
                execution_timeout_seconds: super::vector_index::EXECUTION_TIMEOUT_SECONDS,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    async fn detect(
        &self,
        request: &RunDetectionRequest,
        sender: &dyn DetectionSender,
    ) -> Result<()> {
        let budget = clamp(
            int_or(
                &request.worker_config_values,
                "max_unindexed_rows",
                DEFAULT_MAX_UNINDEXED_ROWS,
            ),
            MAX_UNINDEXED_FLOOR,
            MAX_UNINDEXED_CEILING,
        ) as u64;
        let client = self.client();
        let tables = client.list_all_tables().await?;

        let mut proposals = Vec::new();
        for encoded in &tables {
            let id = parse_id(encoded);
            let table = match dataset::open(&client, &id, &self.storage).await {
                Ok(table) => table,
                Err(err) => {
                    if let Some(counters) = &self.metrics {
                        counters.worker.object_skipped(JOB_TYPE, "open");
                    }
                    warn!("skipping {encoded}: {err:#}");
                    continue;
                }
            };
            if let Some(counters) = &self.metrics {
                counters.worker.object_seen(JOB_TYPE);
            }
            let vector_table = is_vector_table(&id);
            let rows = match index_rows(&table, vector_table).await {
                Ok(Some(rows)) => rows,
                Ok(None) => continue,
                Err(err) => {
                    if let Some(counters) = &self.metrics {
                        counters.worker.object_skipped(JOB_TYPE, "index_stats");
                    }
                    warn!("skipping {encoded}: reading its index stats failed: {err:#}");
                    continue;
                }
            };
            let budget = effective_budget(budget, &rows, vector_table);
            let unindexed = rows.unindexed;
            if unindexed <= budget {
                continue;
            }
            let mut parameters: HashMap<String, ConfigValue> = HashMap::new();
            parameters.insert("table_id".to_string(), string_list(&id));
            if vector_table {
                // The run may turn out to be a retrain.
                super::vector_index::declare_build_runtime(&mut parameters);
            }
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
            skipped_reason: String::new(),
            eligible_after: None,
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
        let mut table = dataset::open(&client, &id, &self.storage).await?;
        let vector_table = is_vector_table(&id);
        let before = index_rows(&table, vector_table)
            .await?
            .map_or(0, |rows| rows.unindexed);
        // A vector index whose table has outgrown its partitions is built
        // again with partitions for the rows it now holds.
        let retrain =
            vector_table && vector_index::retrain_due(&table.dataset, VECTOR_COLUMN).await?;

        sender.send_progress(JobProgressUpdate {
            request_id: request.request_id.clone(),
            job_id: job.job_id.clone(),
            job_type: JOB_TYPE.to_string(),
            progress_percent: 10.0,
            stage: if retrain {
                "retraining the vector index".to_string()
            } else {
                format!("indexing {before} uncovered rows")
            },
            ..Default::default()
        })?;

        if vector_table {
            vector_index::optimize(&mut table.dataset, VECTOR_COLUMN, retrain)
                .await
                .with_context(|| format!("optimize the vector index of {}", table.location))?;
        } else {
            // Merging every delta keeps read latency from drifting as the table
            // is reindexed again and again; leaving them unmerged is how an
            // index ends up fast to write and slow to search.
            table
                .dataset
                .optimize_indices(&OptimizeOptions::default())
                .await
                .with_context(|| format!("optimize indices of {}", table.location))?;
        }

        let after = index_rows(&table, vector_table)
            .await?
            .map_or(0, |rows| rows.unindexed);
        if let Some(counters) = &self.metrics {
            counters.rows_indexed.inc_by(before.saturating_sub(after));
        }
        let mut output: HashMap<String, ConfigValue> = HashMap::new();
        output.insert(
            "unindexed_rows_before".to_string(),
            int_value(before as i64),
        );
        output.insert("unindexed_rows_after".to_string(), int_value(after as i64));
        output.insert(
            "retrained".to_string(),
            seaweed_worker_core::config_form::bool_value(retrain),
        );

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retrain_gets_a_builds_execution_timeout() {
        let descriptor = OptimizeIndicesHandler::new(String::new()).descriptor();
        let defaults = descriptor.admin_runtime_defaults.unwrap();
        assert_eq!(defaults.execution_timeout_seconds, 3600);
        assert_eq!(defaults.job_type_max_runtime_seconds, 3600);
        assert!(!defaults.enabled);
    }

    #[test]
    fn vector_tables_reindex_at_a_tenth_of_their_rows() {
        let small = IndexRows {
            indexed: 60_000,
            unindexed: 0,
        };
        assert_eq!(effective_budget(100_000, &small, true), 6_000);
        assert_eq!(effective_budget(100_000, &small, false), 100_000);
        let large = IndexRows {
            indexed: 5_000_000,
            unindexed: 0,
        };
        assert_eq!(effective_budget(100_000, &large, true), 100_000);
    }
}
