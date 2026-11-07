use std::collections::HashMap;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
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
use crate::jobs::{clamp, string_list, table_id};
use crate::vector_index::{INDEX_NAME, build, index_names, partitions_for};

pub const JOB_TYPE: &str = "lance_vector_index";

const DEFAULT_MIN_ROWS: i64 = 50_000;
const MIN_ROWS_FLOOR: i64 = 1;
const MIN_ROWS_CEILING: i64 = 1_000_000_000;
/// The namespace an S3 Vectors bucket keeps its indexes in.
const VECTOR_NAMESPACE: &str = "vector";
/// The column S3 Vectors writes its vectors to.
const VECTOR_COLUMN: &str = "vector";
/// A build trains on the whole table, so the admin's 90 seconds are not enough.
/// The run window is as long, or it would cut an attempt short first.
pub(crate) const EXECUTION_TIMEOUT_SECONDS: i32 = 3600;

/// Declares a build's time on a proposal. A run started by hand while the
/// schedule is off has no policy to take the timeout from, and the admin
/// gives a job at least the runtime its proposal declares.
pub(crate) fn declare_build_runtime(parameters: &mut HashMap<String, ConfigValue>) {
    parameters.insert(
        "estimated_runtime_seconds".to_string(),
        int_value(EXECUTION_TIMEOUT_SECONDS as i64),
    );
}

/// Builds the vector index of an S3 Vectors table once it holds enough rows
/// for an exact scan to be the slow way. Tables below the threshold, and rows
/// written after a build, are searched exactly; `lance_optimize_indices` then
/// keeps the index covering.
pub struct VectorIndexHandler {
    namespace_url: String,
    storage: dataset::Storage,
    signing_key: Option<SigningKey>,
    metrics: Option<crate::metrics::LanceMetrics>,
}

impl VectorIndexHandler {
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

/// `<bucket>$vector$<index>`: a table the S3 Vectors API created.
pub fn is_vector_table(id: &[String]) -> bool {
    id.len() == 3 && id[1] == VECTOR_NAMESPACE
}

fn has_vector_column(table: &OpenTable) -> bool {
    table.dataset.schema().field(VECTOR_COLUMN).is_some()
}

async fn is_indexed(table: &OpenTable) -> Result<bool> {
    Ok(!index_names(&table.dataset, VECTOR_COLUMN).await?.is_empty())
}

/// Why an execution leaves the table as it is, if it does: the table may have
/// changed since it was proposed.
fn skip_reason(indexed: bool, rows: usize) -> Option<&'static str> {
    if indexed {
        Some("already indexed")
    } else if rows == 0 {
        Some("no rows to index")
    } else {
        None
    }
}

#[async_trait]
impl JobHandler for VectorIndexHandler {
    fn capability(&self) -> JobTypeCapability {
        JobTypeCapability {
            job_type: JOB_TYPE.to_string(),
            can_detect: true,
            can_execute: true,
            max_detection_concurrency: 1,
            max_execution_concurrency: 1,
            display_name: "Lance Vector Index".to_string(),
            description:
                "Build the vector index of S3 Vectors tables that have grown past the threshold"
                    .to_string(),
            weight: 40,
        }
    }

    fn descriptor(&self) -> JobTypeDescriptor {
        let mut defaults: HashMap<String, ConfigValue> = HashMap::new();
        defaults.insert("min_rows".to_string(), int_value(DEFAULT_MIN_ROWS));

        JobTypeDescriptor {
            job_type: JOB_TYPE.to_string(),
            display_name: "Lance Vector Index".to_string(),
            description:
                "Build an IVF index for S3 Vectors tables so queries stop scanning every row"
                    .to_string(),
            icon: "fas fa-project-diagram".to_string(),
            descriptor_version: 1,
            worker_config_form: Some(form(
                "lance-vector-index-worker",
                "Vector index",
                vec![number_field(
                    "min_rows",
                    "Row threshold",
                    "Build the index once a table holds at least this many rows",
                    MIN_ROWS_FLOOR,
                    MIN_ROWS_CEILING,
                )],
                defaults.clone(),
            )),
            worker_default_values: defaults,
            admin_runtime_defaults: Some(AdminRuntimeDefaults {
                job_type_max_runtime_seconds: EXECUTION_TIMEOUT_SECONDS,
                execution_timeout_seconds: EXECUTION_TIMEOUT_SECONDS,
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
        let min_rows = clamp(
            int_or(&request.worker_config_values, "min_rows", DEFAULT_MIN_ROWS),
            MIN_ROWS_FLOOR,
            MIN_ROWS_CEILING,
        ) as usize;
        let max_results = usize::try_from(request.max_results).unwrap_or(0);
        let client = self.client();
        let tables = client.list_all_tables().await?;

        let mut proposals = Vec::new();
        let mut has_more = false;
        for encoded in &tables {
            let id = parse_id(encoded);
            if !is_vector_table(&id) {
                continue;
            }
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
            if !has_vector_column(&table) {
                continue;
            }
            match is_indexed(&table).await {
                Ok(false) => {}
                Ok(true) => continue,
                Err(err) => {
                    if let Some(counters) = &self.metrics {
                        counters.worker.object_skipped(JOB_TYPE, "indices");
                    }
                    warn!("skipping {encoded}: reading its indices failed: {err:#}");
                    continue;
                }
            }
            let rows = match table.dataset.count_rows(None).await {
                Ok(rows) => rows,
                Err(err) => {
                    if let Some(counters) = &self.metrics {
                        counters.worker.object_skipped(JOB_TYPE, "rows");
                    }
                    warn!("skipping {encoded}: counting its rows failed: {err:#}");
                    continue;
                }
            };
            if rows < min_rows {
                continue;
            }
            if max_results > 0 && proposals.len() >= max_results {
                has_more = true;
                break;
            }
            let mut parameters: HashMap<String, ConfigValue> = HashMap::new();
            parameters.insert("table_id".to_string(), string_list(&id));
            declare_build_runtime(&mut parameters);
            proposals.push(JobProposal {
                proposal_id: format!("{JOB_TYPE}:{encoded}"),
                dedupe_key: format!("{JOB_TYPE}:{encoded}"),
                job_type: JOB_TYPE.to_string(),
                summary: format!("Index {encoded} ({rows} rows)"),
                detail: format!(
                    "{rows} rows are searched by an exact scan on every query; the policy \
                     indexes tables from {min_rows} rows"
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
            has_more,
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
        let rows = table.dataset.count_rows(None).await?;
        let partitions = partitions_for(rows);
        let mut output: HashMap<String, ConfigValue> = HashMap::new();
        output.insert("rows".to_string(), int_value(rows as i64));
        output.insert("partitions".to_string(), int_value(partitions as i64));

        if !has_vector_column(&table) {
            bail!("the table has no {VECTOR_COLUMN} column to index");
        }
        if let Some(reason) = skip_reason(is_indexed(&table).await?, rows) {
            sender.send_completed(JobCompleted {
                request_id: request.request_id.clone(),
                job_id: job.job_id.clone(),
                job_type: JOB_TYPE.to_string(),
                success: true,
                result: Some(JobResult {
                    output_values: output,
                    summary: reason.to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            })?;
            return Ok(());
        }

        sender.send_progress(JobProgressUpdate {
            request_id: request.request_id.clone(),
            job_id: job.job_id.clone(),
            job_type: JOB_TYPE.to_string(),
            progress_percent: 10.0,
            stage: format!("indexing {rows} rows into {partitions} partitions"),
            ..Default::default()
        })?;

        build(
            &mut table.dataset,
            VECTOR_COLUMN,
            INDEX_NAME,
            partitions,
            false,
        )
        .await?;
        if let Some(counters) = &self.metrics {
            counters.indexes_built.inc();
        }

        sender.send_completed(JobCompleted {
            request_id: request.request_id.clone(),
            job_id: job.job_id.clone(),
            job_type: JOB_TYPE.to_string(),
            success: true,
            result: Some(JobResult {
                output_values: output,
                summary: format!("{rows} rows indexed into {partitions} partitions"),
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
    fn only_vector_bucket_tables_qualify() {
        assert!(is_vector_table(&parse_id("media$vector$movies")));
        assert!(!is_vector_table(&parse_id("media$ml$events")));
        assert!(!is_vector_table(&parse_id("vector$movies")));
    }

    #[test]
    fn a_table_indexed_or_emptied_since_detection_is_left_alone() {
        assert_eq!(skip_reason(true, 60_000), Some("already indexed"));
        assert_eq!(skip_reason(true, 0), Some("already indexed"));
        assert_eq!(skip_reason(false, 0), Some("no rows to index"));
        assert_eq!(skip_reason(false, 60_000), None);
    }

    #[test]
    fn a_proposal_declares_a_builds_runtime() {
        let mut parameters = HashMap::new();
        declare_build_runtime(&mut parameters);
        assert_eq!(
            seaweed_worker_core::config_form::int_or(&parameters, "estimated_runtime_seconds", 0),
            3600
        );
    }

    #[test]
    fn a_build_gets_longer_than_the_default_execution_timeout() {
        let descriptor = VectorIndexHandler::new(String::new()).descriptor();
        let defaults = descriptor.admin_runtime_defaults.unwrap();
        assert_eq!(defaults.execution_timeout_seconds, 3600);
        assert_eq!(defaults.job_type_max_runtime_seconds, 3600);
        assert!(!defaults.enabled);
    }
}
