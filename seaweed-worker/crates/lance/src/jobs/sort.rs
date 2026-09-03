use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use lance::dataset::scanner::ColumnOrdering;
use lance::dataset::transaction::Operation;
use lance::dataset::write::{CommitBuilder, InsertBuilder};
use lance::dataset::{WriteDestination, WriteMode, WriteParams};
use lance::index::DatasetIndexExt;
use lance_datafusion::exec::{execute_plan, LanceExecutionOptions};
use seaweed_worker_core::config_form::{form, int_or, int_value, string_or, string_value};
use seaweed_worker_core::pb::{
    ConfigValue, DetectionComplete, DetectionProposals, ExecuteJobRequest, JobCompleted,
    JobProgressUpdate, JobProposal, JobResult, JobTypeCapability, JobTypeDescriptor,
    RunDetectionRequest, WorkerObservations,
};
use seaweed_worker_core::{DetectionSender, ExecutionSender, JobHandler};
use seaweed_worker_sort::{
    config_fields, resolve, verdict, FragmentSummary, SortSpec, SortState,
    CONFIG_MAX_ROWS_PER_FILE, CONFIG_MEMORY_BUDGET_MB, CONFIG_MIN_UNSORTED_ROWS,
    CONFIG_SORT_FIELDS, DECLARED_FIELDS_KEY,
};
use tracing::warn;

use crate::catalog::{parse_id, NamespaceClient};
use crate::dataset;
use crate::jobs::{clamp, observation, string_list, table_id, FORMAT};

pub const JOB_TYPE: &str = "lance_sort";

const DEFAULT_MIN_UNSORTED_ROWS: i64 = 1_048_576;
const DEFAULT_MEMORY_BUDGET_MB: i64 = 512;
const DEFAULT_MAX_ROWS_PER_FILE: i64 = 1_048_576;

// The ranges the shared form offers, repeated here because a value from outside
// one is a value the UI could not have produced and every one of these is cast
// to an unsigned type on the way in.
const MIN_UNSORTED_ROWS_FLOOR: i64 = 1;
const MIN_UNSORTED_ROWS_CEILING: i64 = 1_000_000_000;
const MEMORY_BUDGET_FLOOR: i64 = 64;
const MEMORY_BUDGET_CEILING: i64 = 1_048_576;
const MAX_ROWS_PER_FILE_FLOOR: i64 = 1024;
const MAX_ROWS_PER_FILE_CEILING: i64 = 16_777_216;

const BYTES_PER_MB: u64 = 1024 * 1024;

/// Rewrites a table so its rows are stored in the order its fields declare,
/// which is what lets a range scan read a few files instead of all of them.
///
/// Lance appends fragments in write order and has no notion of a sorted table,
/// so nothing but a rewrite establishes that order, and nothing but another
/// rewrite restores it once rows have been appended.
pub struct SortHandler {
    namespace_url: String,
    fallback: dataset::FallbackOptions,
    metrics: Option<crate::metrics::LanceMetrics>,
}

impl SortHandler {
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

/// The fragments a dataset holds, in fragment order, as the sort crate reads
/// them: the data files each one names, and the live rows it holds.
///
/// Appends only add fragments after the existing ones and lance hands out
/// fragment ids monotonically even across an overwrite, so ordering by id makes
/// "the fragments the sort wrote are still the first ones" a prefix test.
fn fragment_summaries(dataset: &lance::Dataset) -> Vec<FragmentSummary> {
    let mut fragments = dataset.get_fragments();
    fragments.sort_by_key(|fragment| fragment.id());
    fragments
        .iter()
        .map(|fragment| summarize(fragment.metadata()))
        .collect()
}

/// The manifest's own account of one fragment. `num_rows` is physical rows less
/// deletions, and None where the manifest does not record them.
fn summarize(fragment: &lance_table::format::Fragment) -> FragmentSummary {
    FragmentSummary {
        files: fragment
            .files
            .iter()
            .map(|file| file.path.clone())
            .collect(),
        rows: fragment.num_rows().map(|rows| rows as u64),
    }
}

/// The scan ordering for a spec. Lance validates the column names against the
/// schema when the ordering is set, so a spec naming a column the table does
/// not have fails before anything is written.
fn orderings(spec: &SortSpec) -> Vec<ColumnOrdering> {
    spec.fields
        .iter()
        .map(|field| ColumnOrdering {
            ascending: !field.descending,
            nulls_first: field.nulls_first,
            column_name: field.path.clone(),
        })
        .collect()
}

#[async_trait]
impl JobHandler for SortHandler {
    fn capability(&self) -> JobTypeCapability {
        JobTypeCapability {
            job_type: JOB_TYPE.to_string(),
            can_detect: true,
            can_execute: true,
            max_detection_concurrency: 1,
            max_execution_concurrency: 1,
            display_name: "Lance Sort".to_string(),
            description: "Rewrite Lance tables in the order their fields declare. The rewrite replaces every fragment, so indices do not survive it and want rebuilding afterwards.".to_string(),
            weight: 40,
        }
    }

    fn descriptor(&self) -> JobTypeDescriptor {
        let mut defaults: HashMap<String, ConfigValue> = HashMap::new();
        defaults.insert(CONFIG_SORT_FIELDS.to_string(), string_value(""));
        defaults.insert(
            CONFIG_MIN_UNSORTED_ROWS.to_string(),
            int_value(DEFAULT_MIN_UNSORTED_ROWS),
        );
        defaults.insert(
            CONFIG_MEMORY_BUDGET_MB.to_string(),
            int_value(DEFAULT_MEMORY_BUDGET_MB),
        );
        defaults.insert(
            CONFIG_MAX_ROWS_PER_FILE.to_string(),
            int_value(DEFAULT_MAX_ROWS_PER_FILE),
        );

        JobTypeDescriptor {
            job_type: JOB_TYPE.to_string(),
            display_name: "Lance Sort".to_string(),
            description: "Sort Lance tables by their declared fields".to_string(),
            icon: "fas fa-sort".to_string(),
            descriptor_version: 1,
            worker_config_form: Some(form(
                "lance-sort-worker",
                "Sort",
                config_fields(),
                defaults.clone(),
            )),
            worker_default_values: defaults,
            ..Default::default()
        }
    }

    /// Propose a job for every table whose declared order the data no longer
    /// follows. Opening a dataset reads its manifest and its configuration, not
    /// its data, so this stays cheap across a catalog.
    async fn detect(
        &self,
        request: &RunDetectionRequest,
        sender: &dyn DetectionSender,
    ) -> Result<()> {
        let configured = string_or(&request.worker_config_values, CONFIG_SORT_FIELDS, "");
        let min_unsorted_rows = clamp(
            int_or(
                &request.worker_config_values,
                CONFIG_MIN_UNSORTED_ROWS,
                DEFAULT_MIN_UNSORTED_ROWS,
            ),
            MIN_UNSORTED_ROWS_FLOOR,
            MIN_UNSORTED_ROWS_CEILING,
        ) as u64;

        let client = self.client();
        let tables = client.list_all_tables().await?;

        let mut proposals = Vec::new();
        let mut observations = Vec::new();
        for encoded in &tables {
            let id = parse_id(encoded);
            let table = match dataset::open(&client, &id, &self.fallback).await {
                Ok(table) => table,
                Err(err) => {
                    // One unreadable table must not end the sweep: the tables
                    // already read would lose their proposals.
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

            let config = table.dataset.config().clone();
            let spec = match resolve(
                config.get(DECLARED_FIELDS_KEY).map(String::as_str),
                &configured,
            ) {
                Ok(Some(spec)) => spec,
                Ok(None) => {
                    // Not every table is one an operator wants sorted, and a
                    // table with no order named anywhere is not a failure.
                    if let Some(counters) = &self.metrics {
                        counters.worker.object_skipped(JOB_TYPE, "no_sort_order");
                    }
                    continue;
                }
                Err(err) => {
                    if let Some(counters) = &self.metrics {
                        counters.worker.object_skipped(JOB_TYPE, "sort_order");
                    }
                    warn!("skipping {encoded}: {err:#}");
                    continue;
                }
            };

            let state = SortState::from_config(&config);
            let decision = verdict(
                &spec,
                &state,
                &fragment_summaries(&table.dataset),
                min_unsorted_rows,
            );

            let mut attributes: HashMap<String, ConfigValue> = HashMap::new();
            attributes.insert("rows".to_string(), int_value(stats.rows as i64));
            attributes.insert("version".to_string(), int_value(stats.version as i64));
            attributes.insert("fragments".to_string(), int_value(stats.fragments as i64));
            attributes.insert("sort_fields".to_string(), string_value(spec.to_string()));
            attributes.insert("sort_state".to_string(), string_value(decision.reason()));
            if let Some(schema) = stats.schema.clone() {
                attributes.insert("schema".to_string(), string_value(schema));
            }
            observations.push(observation(&id, FORMAT, attributes));

            // Logged because "detection proposed nothing" is otherwise
            // indistinguishable from a table the worker could not read.
            tracing::info!(
                "sort detection: {encoded} has {} rows in {spec} order: {}",
                stats.rows,
                decision.reason()
            );

            if !decision.needs_sort() {
                continue;
            }
            let mut parameters: HashMap<String, ConfigValue> = HashMap::new();
            parameters.insert("table_id".to_string(), string_list(&id));
            proposals.push(JobProposal {
                proposal_id: format!("{JOB_TYPE}:{encoded}"),
                dedupe_key: format!("{JOB_TYPE}:{encoded}"),
                job_type: JOB_TYPE.to_string(),
                summary: format!("Sort {encoded} by {spec}"),
                detail: format!("{} rows, {}", stats.rows, decision.reason()),
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

        let configured = string_or(&request.worker_config_values, CONFIG_SORT_FIELDS, "");
        let memory_budget_mb = clamp(
            int_or(
                &request.worker_config_values,
                CONFIG_MEMORY_BUDGET_MB,
                DEFAULT_MEMORY_BUDGET_MB,
            ),
            MEMORY_BUDGET_FLOOR,
            MEMORY_BUDGET_CEILING,
        ) as u64;
        let max_rows_per_file = clamp(
            int_or(
                &request.worker_config_values,
                CONFIG_MAX_ROWS_PER_FILE,
                DEFAULT_MAX_ROWS_PER_FILE,
            ),
            MAX_ROWS_PER_FILE_FLOOR,
            MAX_ROWS_PER_FILE_CEILING,
        ) as usize;

        let client = self.client();
        // Re-resolve rather than trusting what detection saw: the table may have
        // been repointed, its declared order changed, and the vended credentials
        // have expired.
        let table = dataset::open(&client, &id, &self.fallback).await?;
        let before = table.stats().await?;
        let config = table.dataset.config().clone();
        let spec = resolve(
            config.get(DECLARED_FIELDS_KEY).map(String::as_str),
            &configured,
        )?
        .ok_or_else(|| anyhow!("neither the table nor this worker names an order to sort by"))?;

        sender.send_progress(JobProgressUpdate {
            request_id: request.request_id.clone(),
            job_id: job.job_id.clone(),
            job_type: JOB_TYPE.to_string(),
            progress_percent: 10.0,
            stage: format!("sorting {} rows by {spec}", before.rows),
            ..Default::default()
        })?;

        let mut scanner = table.dataset.scan();
        scanner
            .order_by(Some(orderings(&spec)))
            .with_context(|| format!("order the scan of {} by {spec}", table.location))?;
        let plan = scanner
            .create_plan()
            .await
            .context("plan the ordered scan")?;

        // Spilling is off in LanceExecutionOptions::default(), and the scanner's
        // own stream helper takes that default, so the options are built here
        // instead: a sort that cannot spill is one bounded by memory, which is
        // the whole thing this job exists to avoid.
        let stream = execute_plan(
            plan,
            LanceExecutionOptions {
                use_spilling: true,
                mem_pool_size: Some(memory_budget_mb * BYTES_PER_MB),
                ..Default::default()
            },
        )
        .context("run the ordered scan")?;

        let params = WriteParams {
            mode: WriteMode::Overwrite,
            max_rows_per_file,
            ..Default::default()
        };
        let destination = Arc::new(table.dataset.clone());
        let mut transaction = InsertBuilder::new(WriteDestination::Dataset(destination.clone()))
            .with_params(&params)
            .execute_uncommitted_stream(stream)
            .await
            .context("write the sorted fragments")?;

        // The marker rides in the same commit as the data it describes:
        // Operation::Overwrite is the one place lance takes configuration
        // values alongside fragments, and a marker written as a second commit
        // would be lost if the worker died in between, re-sorting the whole
        // table on the next sweep.
        match &mut transaction.operation {
            Operation::Overwrite {
                config_upsert_values,
                fragments,
                ..
            } => {
                // The files these fragments name were written a moment ago and
                // keep their names whatever version this commit becomes, which
                // is what lets the marker travel inside the commit it describes.
                let written: Vec<FragmentSummary> = fragments.iter().map(summarize).collect();
                *config_upsert_values =
                    Some(SortState::record(&spec, &written, before.rows as u64));
            }
            other => {
                return Err(anyhow!(
                    "a sorted rewrite produced {other} instead of an overwrite"
                ))
            }
        }

        let sorted = CommitBuilder::new(WriteDestination::Dataset(destination))
            .execute(transaction)
            .await
            .context("commit the sorted rewrite")?;

        if let Some(counters) = &self.metrics {
            counters.rows_sorted.inc_by(before.rows as u64);
        }

        // An overwrite carries fragments, schema and configuration — not index
        // metadata — so a table that had indices no longer does. Saying so is
        // the difference between a slow table and a mystery.
        let dropped_indices = match table.dataset.load_indices().await {
            Ok(indices) => indices.len(),
            Err(err) => {
                warn!("could not read the indices of {}: {err:#}", table.location);
                0
            }
        };
        if dropped_indices > 0 {
            warn!(
                "{} lost {dropped_indices} index/indices to the sorted rewrite; run the index job to rebuild them",
                table.location
            );
        }

        let fragments_after = sorted.get_fragments().len();
        let mut output: HashMap<String, ConfigValue> = HashMap::new();
        output.insert("rows_sorted".to_string(), int_value(before.rows as i64));
        output.insert("sort_fields".to_string(), string_value(spec.to_string()));
        output.insert(
            "fragments_before".to_string(),
            int_value(before.fragments as i64),
        );
        output.insert(
            "fragments_after".to_string(),
            int_value(fragments_after as i64),
        );
        output.insert(
            "indices_dropped".to_string(),
            int_value(dropped_indices as i64),
        );

        sender.send_completed(JobCompleted {
            request_id: request.request_id.clone(),
            job_id: job.job_id.clone(),
            job_type: JOB_TYPE.to_string(),
            success: true,
            result: Some(JobResult {
                output_values: output,
                summary: format!("{} rows rewritten in {spec} order", before.rows),
                ..Default::default()
            }),
            ..Default::default()
        })?;
        Ok(())
    }
}
