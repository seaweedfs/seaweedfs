use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use seaweed_worker_core::config_form::{form, int_or, int_value, number_field, string_value};
use seaweed_worker_core::pb::{
    AdminRuntimeDefaults, ConfigValue, DetectionComplete, DetectionProposals, ExecuteJobRequest,
    JobCompleted, JobProgressUpdate, JobProposal, JobResult, JobTypeCapability, JobTypeDescriptor,
    RunDetectionRequest, config_value::Kind,
};
use seaweed_worker_core::{DetectionSender, ExecutionSender, JobHandler};
use tracing::warn;

use crate::admin_cluster::AdminCluster;
use crate::catalog::parse_id;
use crate::dataset;
use crate::jobs::vector_index::{
    EXECUTION_TIMEOUT_SECONDS, declare_build_runtime, is_vector_table,
};
use crate::jobs::{clamp, string_list, table_id};
use crate::metadata_index::{self, DEFAULT_MIN_UNINDEXED_ROWS, DEFAULT_SEGMENT_ROWS, State};

pub const JOB_TYPE: &str = "lance_metadata_index";

const SEGMENT_ROWS_FLOOR: i64 = 1_000;
const SEGMENT_ROWS_CEILING: i64 = 1_000_000_000;
const MIN_UNINDEXED_FLOOR: i64 = 0;
const MIN_UNINDEXED_CEILING: i64 = 100_000_000;

/// What a job does to one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Add the requested key's column, or finish a `building` one.
    Add,
    /// Drop an `off` key's index and column.
    Remove,
    /// Fill NULL rows and fold unindexed fragments of a `ready` key.
    Maintain,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Action::Add => "add",
            Action::Remove => "remove",
            Action::Maintain => "maintain",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "add" => Some(Action::Add),
            "remove" => Some(Action::Remove),
            "maintain" => Some(Action::Maintain),
            _ => None,
        }
    }
}

/// The work an index's table asks for: requested and `building` keys to add,
/// `off` keys to remove, and `ready` keys whose unindexed rows reached
/// `min_unindexed_rows`.
pub async fn pending(
    dataset: &lance::Dataset,
    min_unindexed_rows: u64,
) -> Result<Vec<(String, Action)>> {
    let mut out: Vec<(String, Action)> = metadata_index::requests(dataset)
        .into_iter()
        .map(|key| (key, Action::Add))
        .collect();
    for key in metadata_index::keys_of(dataset) {
        let action = match key.state {
            Some(State::Building) => Action::Add,
            Some(State::Off) => Action::Remove,
            Some(State::Ready) => {
                let segments = metadata_index::segments(dataset, &key.column).await?;
                let pending = metadata_index::unindexed(dataset, &segments);
                let unindexed: u64 = pending.iter().map(|(_, rows)| rows).sum();
                if unindexed == 0 {
                    continue;
                }
                // Rows a write left NULL (one that raced the key's add and
                // stopped before fixing them) wait for no threshold: queries
                // through the key miss them until they are derived again.
                // Nor does the first segment of a key that went ready on an
                // empty table.
                if unindexed < min_unindexed_rows
                    && !segments.is_empty()
                    && !has_nulls(dataset, &key.column, &pending).await?
                {
                    continue;
                }
                Action::Maintain
            }
            None => continue,
        };
        out.push((key.key, action));
    }
    Ok(out)
}

/// Whether `fragments` hold a row whose `column` is NULL.
async fn has_nulls(
    dataset: &lance::Dataset,
    column: &str,
    fragments: &[(u32, u64)],
) -> Result<bool> {
    let ids: Vec<u64> = fragments.iter().map(|(id, _)| *id as u64).collect();
    let mut scanner = dataset.scan();
    scanner
        .filter(&format!("`{}` IS NULL", column.replace('`', "``")))?
        .with_fragments(
            dataset
                .fragments()
                .iter()
                .filter(|f| ids.contains(&f.id))
                .cloned()
                .collect(),
        );
    Ok(scanner.count_rows().await? > 0)
}

/// Carries out `action` on `key`, as the table now records it; an action the
/// table no longer asks for is skipped. Returns what was done.
pub async fn run(
    dataset: &mut lance::Dataset,
    key: &str,
    action: Action,
    segment_rows: u64,
    min_unindexed_rows: u64,
    grace: std::time::Duration,
) -> Result<String> {
    let column = metadata_index::keys_of(dataset)
        .into_iter()
        .find(|k| k.key == key);
    match (action, column) {
        (Action::Add, None) => {
            if !metadata_index::requests(dataset).iter().any(|k| k == key) {
                return Ok(format!("{key} is no longer requested"));
            }
            let added = metadata_index::add_column(dataset, key).await?;
            if added.state != Some(State::Building) {
                return Ok(format!(
                    "{key} was withdrawn while {} was added",
                    added.column
                ));
            }
            let filled = metadata_index::finish_after(dataset, &added, segment_rows, grace).await?;
            Ok(format!(
                "{key} indexed in {} ({filled} rows a racing write left derived again)",
                added.column
            ))
        }
        (Action::Add, Some(column)) if column.state == Some(State::Building) => {
            // An add that stopped before setting again what its commit
            // overwrote leaves that to the build resuming it.
            let state = metadata_index::recover_add(dataset, &column).await?;
            if state != Some(State::Building) {
                return Ok(format!(
                    "{key} is {} after its add was recovered",
                    state.map_or("unknown", State::as_str)
                ));
            }
            let filled =
                metadata_index::finish_after(dataset, &column, segment_rows, grace).await?;
            Ok(format!(
                "{key} indexed in {} ({filled} rows a racing write left derived again)",
                column.column
            ))
        }
        (Action::Remove, Some(column)) if column.state == Some(State::Off) => {
            // Dropping the column drops its restore mark: set again first
            // what its add overwrote.
            metadata_index::recover_add(dataset, &column).await?;
            metadata_index::remove_column(dataset, &column.column).await?;
            Ok(format!("{key} removed with {}", column.column))
        }
        (Action::Maintain, Some(column)) if column.state == Some(State::Ready) => {
            let (filled, folded) =
                metadata_index::maintain(dataset, &column, segment_rows, min_unindexed_rows)
                    .await?;
            Ok(format!("{key}: {folded:?}, {filled} rows derived again"))
        }
        (action, column) => Ok(format!(
            "{key} needs no {}: {}",
            action.as_str(),
            column
                .and_then(|c| c.state)
                .map_or("not indexed", State::as_str)
        )),
    }
}

/// Adds, maintains and removes the indexed metadata keys of S3 Vectors
/// indexes, as each index's table records them. The admin dispatches a job
/// when an operator asks; detection finds the rest.
pub struct MetadataIndexHandler {
    cluster: Arc<AdminCluster>,
    metrics: Option<crate::metrics::LanceMetrics>,
}

impl MetadataIndexHandler {
    pub fn new(cluster: Arc<AdminCluster>) -> Self {
        Self {
            cluster,
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Option<crate::metrics::LanceMetrics>) -> Self {
        self.metrics = metrics;
        self
    }
}

fn settings(values: &HashMap<String, ConfigValue>) -> (u64, u64) {
    (
        clamp(
            int_or(values, "segment_rows", DEFAULT_SEGMENT_ROWS as i64),
            SEGMENT_ROWS_FLOOR,
            SEGMENT_ROWS_CEILING,
        ) as u64,
        clamp(
            int_or(
                values,
                "min_unindexed_rows",
                DEFAULT_MIN_UNINDEXED_ROWS as i64,
            ),
            MIN_UNINDEXED_FLOOR,
            MIN_UNINDEXED_CEILING,
        ) as u64,
    )
}

fn string_param(parameters: &HashMap<String, ConfigValue>, name: &str) -> Option<String> {
    match parameters.get(name)?.kind.as_ref()? {
        Kind::StringValue(s) => Some(s.clone()),
        _ => None,
    }
}

#[async_trait]
impl JobHandler for MetadataIndexHandler {
    fn capability(&self) -> JobTypeCapability {
        JobTypeCapability {
            job_type: JOB_TYPE.to_string(),
            can_detect: true,
            can_execute: true,
            max_detection_concurrency: 1,
            max_execution_concurrency: 1,
            display_name: "Lance Metadata Index".to_string(),
            description: "Add, maintain and remove the indexed metadata keys of S3 Vectors indexes"
                .to_string(),
            weight: 40,
        }
    }

    fn descriptor(&self) -> JobTypeDescriptor {
        let mut defaults: HashMap<String, ConfigValue> = HashMap::new();
        defaults.insert(
            "segment_rows".to_string(),
            int_value(DEFAULT_SEGMENT_ROWS as i64),
        );
        defaults.insert(
            "min_unindexed_rows".to_string(),
            int_value(DEFAULT_MIN_UNINDEXED_ROWS as i64),
        );
        JobTypeDescriptor {
            job_type: JOB_TYPE.to_string(),
            display_name: "Lance Metadata Index".to_string(),
            description:
                "Keep a LABEL_LIST index for each metadata key an S3 Vectors index asks for"
                    .to_string(),
            icon: "fas fa-tags".to_string(),
            descriptor_version: 1,
            worker_config_form: Some(form(
                "lance-metadata-index-worker",
                "Metadata index",
                vec![
                    number_field(
                        "segment_rows",
                        "Segment rows",
                        "Rows an index segment takes before a new one starts",
                        SEGMENT_ROWS_FLOOR,
                        SEGMENT_ROWS_CEILING,
                    ),
                    number_field(
                        "min_unindexed_rows",
                        "Unindexed rows",
                        "Rows written since the last fold that wait for the next one",
                        MIN_UNINDEXED_FLOOR,
                        MIN_UNINDEXED_CEILING,
                    ),
                ],
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
        let (_, min_unindexed_rows) = settings(&request.worker_config_values);
        let max_results = usize::try_from(request.max_results).unwrap_or(0);
        let client = self.cluster.client()?;
        let storage = self.cluster.storage()?;
        let mut proposals = Vec::new();
        let mut has_more = false;
        'tables: for encoded in &client.list_all_tables().await? {
            let id = parse_id(encoded);
            if !is_vector_table(&id) {
                continue;
            }
            let table = match dataset::open(&client, &id, &storage).await {
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
            let work = match pending(&table.dataset, min_unindexed_rows).await {
                Ok(work) => work,
                Err(err) => {
                    warn!("skipping {encoded}: reading its indexed keys failed: {err:#}");
                    continue;
                }
            };
            for (key, action) in work {
                if max_results > 0 && proposals.len() >= max_results {
                    has_more = true;
                    break 'tables;
                }
                proposals.push(proposal(&id, encoded, &key, action));
            }
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
        if !is_vector_table(&id) {
            bail!("{} is not an S3 Vectors index", id.join("/"));
        }
        let key = string_param(&job.parameters, "key")
            .ok_or_else(|| anyhow!("job {} carried no key", job.job_id))?;
        let action = string_param(&job.parameters, "action")
            .as_deref()
            .and_then(Action::parse)
            .ok_or_else(|| anyhow!("job {} carried no valid action", job.job_id))?;
        let (segment_rows, min_unindexed_rows) = settings(&request.worker_config_values);
        sender.send_progress(JobProgressUpdate {
            request_id: request.request_id.clone(),
            job_id: job.job_id.clone(),
            job_type: JOB_TYPE.to_string(),
            progress_percent: 10.0,
            stage: format!("{} {key}", action.as_str()),
            ..Default::default()
        })?;
        let client = self.cluster.client()?;
        let storage = self.cluster.storage()?;
        let mut table = dataset::open(&client, &id, &storage).await?;
        let summary = run(
            &mut table.dataset,
            &key,
            action,
            segment_rows,
            min_unindexed_rows,
            metadata_index::RACE_GRACE,
        )
        .await?;
        sender.send_completed(JobCompleted {
            request_id: request.request_id.clone(),
            job_id: job.job_id.clone(),
            job_type: JOB_TYPE.to_string(),
            success: true,
            result: Some(JobResult {
                summary,
                ..Default::default()
            }),
            ..Default::default()
        })?;
        Ok(())
    }
}

/// The proposal for one key of one table; the admin builds the same
/// parameters when it dispatches a job for an operator.
fn proposal(id: &[String], encoded: &str, key: &str, action: Action) -> JobProposal {
    let mut parameters: HashMap<String, ConfigValue> = HashMap::new();
    parameters.insert("table_id".to_string(), string_list(id));
    parameters.insert("key".to_string(), string_value(key));
    parameters.insert("action".to_string(), string_value(action.as_str()));
    declare_build_runtime(&mut parameters);
    let dedupe = format!("{JOB_TYPE}:{encoded}:{key}:{}", action.as_str());
    JobProposal {
        proposal_id: dedupe.clone(),
        dedupe_key: dedupe,
        job_type: JOB_TYPE.to_string(),
        summary: format!("{} metadata key {key} of {encoded}", action.as_str()),
        parameters,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_proposal_names_the_table_key_and_action() {
        let id = parse_id("media$vector$movies");
        let p = proposal(&id, "media$vector$movies", "author", Action::Remove);
        assert_eq!(table_id(&p.parameters), Some(id));
        assert_eq!(
            string_param(&p.parameters, "key").as_deref(),
            Some("author")
        );
        assert_eq!(
            string_param(&p.parameters, "action")
                .as_deref()
                .and_then(Action::parse),
            Some(Action::Remove)
        );
        assert_eq!(
            p.dedupe_key,
            "lance_metadata_index:media$vector$movies:author:remove"
        );
    }

    #[test]
    fn settings_are_held_to_their_ranges() {
        let mut values = HashMap::new();
        assert_eq!(
            settings(&values),
            (DEFAULT_SEGMENT_ROWS, DEFAULT_MIN_UNINDEXED_ROWS)
        );
        values.insert("segment_rows".to_string(), int_value(-5));
        values.insert("min_unindexed_rows".to_string(), int_value(-1));
        assert_eq!(settings(&values), (SEGMENT_ROWS_FLOOR as u64, 0));
    }

    /// A removal of a key whose add stopped before setting again what it
    /// overwrote sets that first: a key requested meanwhile stays requested.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_removal_keeps_what_the_add_overwrote() {
        use crate::metadata_index::tests::lifecycle::table;
        let dir = tempfile::TempDir::new().unwrap();
        let mut dataset = table(&dir, 2).await;
        metadata_index::request(&mut dataset, "author")
            .await
            .unwrap();
        let mut adder = dataset.clone();
        metadata_index::request(&mut dataset, "lang").await.unwrap();
        metadata_index::commit_column(&mut adder, "author")
            .await
            .unwrap();
        metadata_index::request_removal(&mut adder, "author")
            .await
            .unwrap();
        run(
            &mut adder,
            "author",
            Action::Remove,
            1_000,
            0,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
        assert!(metadata_index::keys_of(&adder).is_empty());
        assert_eq!(metadata_index::requests(&adder), ["lang"]);
    }

    /// A withdrawal of a request an interrupted add overwrote sets that
    /// first, so recovering the add does not bring the request back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_withdrawal_of_a_request_an_add_overwrote_stays_withdrawn() {
        use crate::metadata_index::tests::lifecycle::table;
        let dir = tempfile::TempDir::new().unwrap();
        let mut dataset = table(&dir, 2).await;
        metadata_index::request(&mut dataset, "author")
            .await
            .unwrap();
        let mut adder = dataset.clone();
        metadata_index::request(&mut dataset, "lang").await.unwrap();
        metadata_index::commit_column(&mut adder, "author")
            .await
            .unwrap();
        dataset.checkout_latest().await.unwrap();
        metadata_index::request_removal(&mut dataset, "lang")
            .await
            .unwrap();
        let author = metadata_index::keys_of(&dataset).remove(0);
        metadata_index::recover_add(&mut dataset, &author)
            .await
            .unwrap();
        assert!(metadata_index::requests(&dataset).is_empty());
    }

    /// A withdrawal that read the request before the key's column was added
    /// and committed after sets the column off.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_withdrawal_racing_the_add_sets_the_column_off() {
        use crate::metadata_index::tests::lifecycle::table;
        let dir = tempfile::TempDir::new().unwrap();
        let mut dataset = table(&dir, 2).await;
        metadata_index::request(&mut dataset, "genre")
            .await
            .unwrap();
        let mut operator = dataset.clone();
        let added = metadata_index::add_column(&mut dataset, "genre")
            .await
            .unwrap();
        assert_eq!(added.state, Some(State::Building));
        metadata_index::request_removal(&mut operator, "genre")
            .await
            .unwrap();
        dataset.checkout_latest().await.unwrap();
        assert_eq!(metadata_index::keys_of(&dataset)[0].state, Some(State::Off));
        assert!(metadata_index::requests(&dataset).is_empty());
    }

    /// A key that went ready on an empty table builds its first segment
    /// once rows arrive, below the threshold.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_key_ready_without_a_segment_is_maintained_at_once() {
        use crate::metadata_index::tests::lifecycle::{table, upsert};
        let dir = tempfile::TempDir::new().unwrap();
        let mut dataset = table(&dir, 1).await;
        dataset.delete("true").await.unwrap();
        metadata_index::request(&mut dataset, "author")
            .await
            .unwrap();
        run(
            &mut dataset,
            "author",
            Action::Add,
            1_000,
            0,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
        assert_eq!(
            metadata_index::keys_of(&dataset)[0].state,
            Some(State::Ready)
        );
        assert!(pending(&dataset, 1_000_000).await.unwrap().is_empty());

        let mut dataset = upsert(dataset, 0..100).await;
        assert_eq!(
            pending(&dataset, 1_000_000).await.unwrap(),
            [("author".to_string(), Action::Maintain)]
        );
        run(
            &mut dataset,
            "author",
            Action::Maintain,
            1_000,
            1_000_000,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
        assert_eq!(
            metadata_index::segments(&dataset, "_mk_0")
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(pending(&dataset, 1_000_000).await.unwrap().is_empty());
    }

    /// What the table asks for is what a job does, and a job the table no
    /// longer asks for does nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn jobs_follow_what_the_table_records() {
        use crate::metadata_index::tests::lifecycle::{table, upsert};
        let dir = tempfile::TempDir::new().unwrap();
        let mut dataset = table(&dir, 4).await;
        assert!(pending(&dataset, 0).await.unwrap().is_empty());
        metadata_index::request(&mut dataset, "author")
            .await
            .unwrap();
        metadata_index::request(&mut dataset, "genre")
            .await
            .unwrap();
        assert_eq!(
            pending(&dataset, 0).await.unwrap(),
            [
                ("author".to_string(), Action::Add),
                ("genre".to_string(), Action::Add)
            ]
        );
        metadata_index::request_removal(&mut dataset, "genre")
            .await
            .unwrap();
        let skipped = run(
            &mut dataset,
            "genre",
            Action::Add,
            1_000,
            0,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
        assert_eq!(skipped, "genre is no longer requested");
        run(
            &mut dataset,
            "author",
            Action::Add,
            1_000,
            0,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
        let keys = metadata_index::keys_of(&dataset);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].state, Some(State::Ready));
        assert_eq!(
            metadata_index::segments(&dataset, "_mk_0")
                .await
                .unwrap()
                .len(),
            4
        );
        assert!(pending(&dataset, 0).await.unwrap().is_empty());

        let mut dataset = upsert(dataset, 4_000..4_500).await;
        assert!(pending(&dataset, 1_000).await.unwrap().is_empty());
        assert_eq!(
            pending(&dataset, 500).await.unwrap(),
            [("author".to_string(), Action::Maintain)]
        );
        run(
            &mut dataset,
            "author",
            Action::Maintain,
            1_000,
            500,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
        assert!(pending(&dataset, 0).await.unwrap().is_empty());

        // A raced write that left NULL rows is maintained at once.
        let stale = dataset.clone();
        let mut dataset =
            crate::metadata_index::tests::lifecycle::upsert_with_null_columns(stale, 4_500..4_520)
                .await;
        assert_eq!(
            pending(&dataset, 1_000_000).await.unwrap(),
            [("author".to_string(), Action::Maintain)]
        );
        run(
            &mut dataset,
            "author",
            Action::Maintain,
            1_000,
            1_000_000,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
        assert!(pending(&dataset, 1_000_000).await.unwrap().is_empty());
        let noop = run(
            &mut dataset,
            "author",
            Action::Remove,
            1_000,
            0,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
        assert_eq!(noop, "author needs no remove: ready");

        metadata_index::request_removal(&mut dataset, "author")
            .await
            .unwrap();
        assert_eq!(
            pending(&dataset, 0).await.unwrap(),
            [("author".to_string(), Action::Remove)]
        );
        run(
            &mut dataset,
            "author",
            Action::Remove,
            1_000,
            0,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
        assert!(metadata_index::keys_of(&dataset).is_empty());
        assert!(pending(&dataset, 0).await.unwrap().is_empty());
    }
}
