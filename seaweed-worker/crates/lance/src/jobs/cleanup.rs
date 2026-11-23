use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use lance::Dataset;
use lance::dataset::cleanup::{CleanupPolicy, cleanup_old_versions};
use seaweed_worker_core::config_form::{form, int_or, int_value, number_field};
use seaweed_worker_core::pb::{
    AdminRuntimeDefaults, ConfigValue, DetectionComplete, DetectionProposals, ExecuteJobRequest,
    JobCompleted, JobProgressUpdate, JobProposal, JobResult, JobTypeCapability, JobTypeDescriptor,
    RunDetectionRequest,
};
use seaweed_worker_core::{DetectionSender, ExecutionSender, JobHandler};
use tracing::warn;

use crate::admin_cluster::AdminCluster;
use crate::catalog::parse_id;
use crate::dataset;
use crate::jobs::purge::{self, Retention};
use crate::jobs::{clamp, string_list, table_id};

pub const JOB_TYPE: &str = "lance_cleanup_versions";

const DEFAULT_RETAIN_HOURS: i64 = 168;
const DEFAULT_MIN_VERSIONS: i64 = 5;

// The ranges the form offers. Duration::hours also panics far outside this one.
const MAX_RETAIN_HOURS: i64 = 8760;
const MAX_MIN_VERSIONS: i64 = 1000;

/// Lance keeps every version until something removes it. Lance can also do this
/// itself through auto-cleanup, so this job is for deployments that would rather
/// the cluster owned the policy than each writer. A table may override the
/// retention in its own config, and may ask for a purge (see [`purge`]), which
/// this job carries out ahead of the retention window.
pub struct CleanupVersionsHandler {
    cluster: Arc<AdminCluster>,
    metrics: Option<crate::metrics::LanceMetrics>,
}

impl CleanupVersionsHandler {
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

/// The oldest version that may be removed while still leaving `min_versions`
/// behind, or None when the table has no more than the floor.
async fn version_floor(dataset: &Dataset, min_versions: usize) -> Result<Option<u64>> {
    let mut versions: Vec<u64> = dataset
        .versions()
        .await?
        .iter()
        .map(|version| version.version)
        .collect();
    if versions.len() <= min_versions {
        return Ok(None);
    }
    versions.sort_unstable();
    Ok(Some(versions[versions.len() - min_versions]))
}

/// The cleanup that keeps `min_versions` and every version younger than
/// `retain_hours`, or None when the floor leaves nothing to remove. A purge
/// cleans up through it too.
pub async fn retention_policy(
    dataset: &Dataset,
    retain_hours: i64,
    min_versions: usize,
) -> Result<Option<CleanupPolicy>> {
    let Some(floor) = version_floor(dataset, min_versions).await? else {
        return Ok(None);
    };
    Ok(Some(CleanupPolicy {
        before_timestamp: Some(Utc::now() - Duration::hours(retain_hours)),
        // should_clean() ANDs its clauses, so a version has to be both older
        // than the window and below the floor to go.
        before_version: Some(floor),
        // Files this dataset cannot account for are left alone: they may
        // belong to a writer that has not committed yet, and deleting them
        // would corrupt a commit in flight.
        delete_unverified: false,
        // A tagged version is pinned on purpose, so refuse rather than
        // silently dropping what someone named.
        error_if_tagged_old_versions: true,
        ..Default::default()
    }))
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
            // A purge compacts and retrains, so it gets a build's time.
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
        let min_versions = clamp(
            int_or(
                &request.worker_config_values,
                "min_versions_to_keep",
                DEFAULT_MIN_VERSIONS,
            ),
            1,
            MAX_MIN_VERSIONS,
        ) as usize;
        let client = self.cluster.client()?;
        let storage = self.cluster.storage()?;
        let tables = client.list_all_tables().await?;

        let mut proposals = Vec::new();
        for encoded in &tables {
            let id = parse_id(encoded);
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
            let config = table.dataset.config();
            let pending_purge = purge::pending(config);
            let floor = Retention::for_table(
                config,
                Retention {
                    retain_hours: DEFAULT_RETAIN_HOURS,
                    min_versions,
                },
                MAX_RETAIN_HOURS,
                MAX_MIN_VERSIONS,
            )
            .min_versions;
            // Age is decided at execution against the retention window; a table
            // at or under the floor cannot lose a version whatever its age, so
            // proposing one would only produce a job with nothing to do. A
            // requested purge ignores the floor: it keeps only the latest.
            if pending_purge.is_none() && stats.total_versions <= floor {
                continue;
            }
            let mut parameters: HashMap<String, ConfigValue> = HashMap::new();
            parameters.insert("table_id".to_string(), string_list(&id));
            proposals.push(JobProposal {
                proposal_id: format!("{JOB_TYPE}:{encoded}"),
                dedupe_key: format!("{JOB_TYPE}:{encoded}"),
                job_type: JOB_TYPE.to_string(),
                summary: match &pending_purge {
                    Some(_) => format!("Purge {encoded} ({} versions)", stats.total_versions),
                    None => format!("Clean up {encoded} ({} versions)", stats.total_versions),
                },
                detail: match &pending_purge {
                    Some(token) => format!(
                        "purge requested at {token}: rewrite fragments with deletions \
                         and keep only the latest version"
                    ),
                    None => format!(
                        "{} versions retained, above the {floor} floor; \
                         those outside the retention window can go",
                        stats.total_versions
                    ),
                },
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
        let retain_hours = clamp(
            int_or(
                &request.worker_config_values,
                "retain_hours",
                DEFAULT_RETAIN_HOURS,
            ),
            0,
            MAX_RETAIN_HOURS,
        );
        let min_versions = clamp(
            int_or(
                &request.worker_config_values,
                "min_versions_to_keep",
                DEFAULT_MIN_VERSIONS,
            ),
            1,
            MAX_MIN_VERSIONS,
        ) as usize;

        let client = self.cluster.client()?;
        let storage = self.cluster.storage()?;
        let mut table = dataset::open(&client, &id, &storage).await?;
        let before = table.stats().await?;

        if let Some(token) = purge::pending(table.dataset.config()) {
            return self
                .execute_purge(request, sender, &job.job_id, &mut table, &token)
                .await;
        }
        let Retention {
            retain_hours,
            min_versions,
        } = Retention::for_table(
            table.dataset.config(),
            Retention {
                retain_hours,
                min_versions,
            },
            MAX_RETAIN_HOURS,
            MAX_MIN_VERSIONS,
        );

        // The floor is a promise about how much history survives, so it has to
        // be applied here and not only when the job was proposed: by the time it
        // runs, versions may have aged past the retention window, and age alone
        // would take the table below what the operator asked to keep.
        let Some(policy) = retention_policy(&table.dataset, retain_hours, min_versions).await?
        else {
            sender.send_completed(JobCompleted {
                request_id: request.request_id.clone(),
                job_id: job.job_id.clone(),
                job_type: JOB_TYPE.to_string(),
                success: true,
                result: Some(JobResult {
                    summary: format!(
                        "kept all {} versions; the {min_versions} version floor leaves none to remove",
                        before.total_versions
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            })?;
            return Ok(());
        };

        sender.send_progress(JobProgressUpdate {
            request_id: request.request_id.clone(),
            job_id: job.job_id.clone(),
            job_type: JOB_TYPE.to_string(),
            progress_percent: 10.0,
            stage: format!("cleaning up {} versions", before.total_versions),
            ..Default::default()
        })?;

        let stats = cleanup_old_versions(&table.dataset, policy)
            .await
            .with_context(|| format!("clean up versions of {}", table.location))?;

        if let Some(counters) = &self.metrics {
            counters.versions_removed.inc_by(stats.old_versions as u64);
            counters.bytes_reclaimed.inc_by(stats.bytes_removed as u64);
        }

        let mut output: HashMap<String, ConfigValue> = HashMap::new();
        output.insert(
            "old_versions_removed".to_string(),
            int_value(stats.old_versions as i64),
        );
        output.insert(
            "bytes_removed".to_string(),
            int_value(stats.bytes_removed as i64),
        );
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

impl CleanupVersionsHandler {
    async fn execute_purge(
        &self,
        request: &ExecuteJobRequest,
        sender: &dyn ExecutionSender,
        job_id: &str,
        table: &mut dataset::OpenTable,
        token: &str,
    ) -> Result<()> {
        let stats = purge::purge(&mut table.dataset, token)
            .await
            .with_context(|| format!("purge {}", table.location))?;
        if let Some(counters) = &self.metrics {
            counters.versions_removed.inc_by(stats.old_versions);
            counters.bytes_reclaimed.inc_by(stats.bytes_removed);
        }
        let mut output: HashMap<String, ConfigValue> = HashMap::new();
        output.insert(
            "fragments_rewritten".to_string(),
            int_value(stats.fragments_rewritten as i64),
        );
        output.insert(
            "old_versions_removed".to_string(),
            int_value(stats.old_versions as i64),
        );
        output.insert(
            "bytes_removed".to_string(),
            int_value(stats.bytes_removed as i64),
        );
        sender.send_completed(JobCompleted {
            request_id: request.request_id.clone(),
            job_id: job_id.to_string(),
            job_type: JOB_TYPE.to_string(),
            success: true,
            result: Some(JobResult {
                output_values: output,
                summary: if stats.served {
                    format!(
                        "purged: rewrote {} fragments, removed {} versions and {} bytes",
                        stats.fragments_rewritten, stats.old_versions, stats.bytes_removed
                    )
                } else {
                    format!(
                        "purge pending: rewrote {} fragments, removed {} versions and {} bytes; \
                         versions from before the rebuild are still retained",
                        stats.fragments_rewritten, stats.old_versions, stats.bytes_removed
                    )
                },
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
    fn a_purge_gets_a_builds_execution_timeout() {
        let descriptor = CleanupVersionsHandler::new(Default::default()).descriptor();
        let defaults = descriptor.admin_runtime_defaults.unwrap();
        assert_eq!(defaults.execution_timeout_seconds, 3600);
        assert_eq!(defaults.job_type_max_runtime_seconds, 3600);
        assert!(!defaults.enabled);
    }
}
