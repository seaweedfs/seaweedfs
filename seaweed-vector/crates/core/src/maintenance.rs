//! Snapshot discipline under Lance maintenance: queries pin a dataset version,
//! and version tags act as the lease maintenance must respect.

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use lance::Dataset;
use lance::dataset::refs::Ref;
use uuid::Uuid;

const QUERY_TAG_PREFIX: &str = "weed-vector-query-";
/// A lease outliving this is treated as abandoned by a dead or cancelled
/// request and reaped by the next [`pin`].
const LEASE_TTL: Duration = Duration::from_secs(3600);
/// Foreign leases — another server's live queries or a dead process's
/// leftovers — get this much longer backstop; a live query on a different
/// server keeps its pin.
const FOREIGN_TTL: Duration = Duration::from_secs(24 * 3600);
/// Expired-lease scans run at most once per interval per dataset, not per
/// request.
const REAP_INTERVAL: Duration = Duration::from_secs(60);
/// A process-unique owner id embedded in lease tags so a reaper only applies
/// the short TTL to its own leases.
static INSTANCE: LazyLock<String> = LazyLock::new(|| Uuid::new_v4().simple().to_string());
/// Nanos since the Unix epoch when each dataset last had its tags scanned,
/// keyed by dataset uri.
static LAST_REAP: LazyLock<Mutex<HashMap<String, u128>>> = LazyLock::new(Mutex::default);
/// Tags this process still holds. An expired tag is only reaped once no live
/// lease here owns it, so a long-running query keeps its pin.
static LIVE_TAGS: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(Mutex::default);

/// A pinned dataset version. The lease tag is removed on drop; call
/// [`Lease::release`] to await removal and observe failures.
pub struct Lease {
    dataset: Dataset,
    tag: String,
}

impl Lease {
    /// Remove the lease tag now instead of on drop.
    pub async fn release(mut self) -> Result<()> {
        let tag = std::mem::take(&mut self.tag);
        untrack(&tag);
        release(&self.dataset, &tag).await
    }

    /// Hand the tag name off and skip the drop-time release — a distributed
    /// planner keeps the version pinned until an executor takes over. The
    /// tag leaves LIVE_TAGS so an abandoned handoff still expires at the
    /// lease TTL instead of surviving forever locally.
    pub fn forget(mut self) -> String {
        let tag = std::mem::take(&mut self.tag);
        untrack(&tag);
        tag
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if self.tag.is_empty() {
            return;
        }
        let tag = std::mem::take(&mut self.tag);
        untrack(&tag);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let dataset = self.dataset.clone();
            handle.spawn(async move {
                if let Err(err) = release(&dataset, &tag).await {
                    tracing::warn!("releasing query lease {tag}: {err:#}");
                }
            });
        }
    }
}

/// Lease-pin `dataset`'s current version with a tag so cleanup skips it.
pub async fn pin(dataset: &Dataset) -> Result<Lease> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    reap_expired(dataset, now).await;
    let tag = format!(
        "{QUERY_TAG_PREFIX}{instance}-{now}-{}",
        Uuid::new_v4().simple(),
        instance = &*INSTANCE,
    );
    let version = dataset.version().version;
    dataset
        .tags()
        .create(&tag, Ref::Version(None, Some(version)))
        .await?;
    // A cleanup that listed tags before this create can still reclaim the
    // version; fail the read now rather than mid-query.
    if dataset.checkout_version(version).await.is_err() {
        let _ = dataset.tags().delete(&tag).await;
        anyhow::bail!("dataset version {version} was removed before it could be leased");
    }
    if let Ok(mut live) = LIVE_TAGS.lock() {
        live.insert(tag.clone());
    }
    Ok(Lease {
        dataset: dataset.clone(),
        tag,
    })
}

/// [`pin`] that answers the query anyway when the storage identity may read
/// but not write: a denied tag write degrades to an unleased query rather
/// than an error.
pub async fn try_pin(dataset: &Dataset) -> Result<Option<Lease>> {
    match pin(dataset).await {
        Ok(lease) => Ok(Some(lease)),
        Err(err) if denied(&err) => {
            tracing::warn!("query runs without a version lease: {err:#}");
            Ok(None)
        }
        Err(err) => Err(err),
    }
}

fn denied(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<object_store::Error>(),
            Some(object_store::Error::PermissionDenied { .. })
                | Some(object_store::Error::Unauthenticated { .. })
        )
    })
}

async fn release(dataset: &Dataset, tag: &str) -> Result<()> {
    Ok(dataset.tags().delete(tag).await?)
}

fn untrack(tag: &str) {
    if let Ok(mut live) = LIVE_TAGS.lock() {
        live.remove(tag);
    }
}

/// Drop lease tags whose owner never released them, so an abandoned lease
/// cannot keep a version out of cleanup forever.
async fn reap_expired(dataset: &Dataset, now: u128) {
    {
        let Ok(mut scanned) = LAST_REAP.lock() else {
            return;
        };
        if now.saturating_sub(*scanned.get(dataset.uri()).unwrap_or(&0)) <= REAP_INTERVAL.as_nanos()
        {
            return;
        }
        if scanned.len() >= 4096 {
            scanned.clear();
        }
        scanned.insert(dataset.uri().to_string(), now);
    }
    let Ok(tags) = dataset.tags().list().await else {
        return;
    };
    for name in tags.keys() {
        let held = LIVE_TAGS
            .lock()
            .map(|live| live.contains(name))
            .unwrap_or(false);
        // {instance}-{ts}-{uuid} is the owned format; {ts}-{uuid} is a
        // legacy foreign tag. Segment count picks the format — an all-digit
        // owner parses as a number too. Own leases use the short TTL;
        // foreign ones only the long backstop.
        let expired = name
            .strip_prefix(QUERY_TAG_PREFIX)
            .and_then(|rest| {
                let mut it = rest.splitn(3, '-');
                let a = it.next()?;
                let b = it.next()?;
                match it.next() {
                    Some(_) => Some((a == *INSTANCE, b.parse::<u128>().ok()?)),
                    None => Some((false, a.parse::<u128>().ok()?)),
                }
            })
            .is_some_and(|(own, stamp)| {
                now.saturating_sub(stamp)
                    > if own { LEASE_TTL } else { FOREIGN_TTL }.as_nanos()
            });
        if expired && !held {
            let _ = dataset.tags().delete(name).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Query, VectorTable, query, testing};
    use lance::dataset::cleanup::{CleanupPolicy, cleanup_old_versions};
    use lance::dataset::optimize::{CompactionOptions, compact_files};
    use lance_linalg::distance::DistanceType;
    use tempfile::TempDir;

    async fn pinned_hits(dataset: &Dataset, version: u64, query_vector: &[f32]) -> Vec<u64> {
        let pinned = dataset.checkout_version(version).await.unwrap();
        let resolved = VectorTable::resolve(&pinned, None, None).unwrap();
        let q = Query {
            vector: query_vector.to_vec(),
            k: 3,
            filter: None,
            postfilter: false,
            distance: DistanceType::L2,
            nprobes: None,
            index_segments: None,
            fragments: None,
        };
        let mut ids: Vec<u64> = query::flat(&pinned, &resolved, &q)
            .await
            .unwrap()
            .iter()
            .map(|h| h.row_id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Compaction commits a new version and rewrites fragments; a query pinned
    /// at the old version still reads it.
    #[tokio::test]
    async fn pinned_query_survives_compaction() {
        let dir = TempDir::new().unwrap();
        let mut dataset =
            testing::write_dataset(&dir, &testing::vectors(4, 4), &["a", "b", "c", "d"]).await;
        testing::append_dataset(&mut dataset, &testing::vectors(4, 4), &["e", "f", "g", "h"]).await;
        let v1 = dataset.version().version;
        let before = pinned_hits(&dataset, v1, &[0.0, 0.0, 0.0, 1.0]).await;

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 64,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        assert!(dataset.version().version > v1);
        assert_eq!(
            pinned_hits(&dataset, v1, &[0.0, 0.0, 0.0, 1.0]).await,
            before
        );
    }

    /// A pinned version carrying a query tag is skipped by cleanup; removing
    /// the lease first lets cleanup delete it.
    #[tokio::test]
    async fn tagged_version_survives_cleanup() {
        let dir = TempDir::new().unwrap();
        let mut dataset =
            testing::write_dataset(&dir, &testing::vectors(4, 4), &["a", "b", "c", "d"]).await;
        let v1 = dataset.version().version;
        let before = pinned_hits(&dataset, v1, &[0.0, 0.0, 0.0, 1.0]).await;
        let lease = pin(&dataset).await.unwrap();

        testing::append_dataset(&mut dataset, &testing::vectors(4, 4), &["e", "f", "g", "h"]).await;
        let v2 = dataset.version().version;

        // Tagged old versions refuse to clean by default.
        let policy = CleanupPolicy {
            before_version: Some(v2),
            delete_unverified: true,
            error_if_tagged_old_versions: true,
            clean_referenced_branches: false,
            delete_rate_limit: None,
            ..Default::default()
        };
        cleanup_old_versions(&dataset, policy.clone())
            .await
            .unwrap_err();

        let policy = CleanupPolicy {
            error_if_tagged_old_versions: false,
            ..policy
        };
        cleanup_old_versions(&dataset, policy).await.unwrap();
        assert_eq!(
            pinned_hits(&dataset, v1, &[0.0, 0.0, 0.0, 1.0]).await,
            before
        );

        // Lease released: the old version is now fair game.
        lease.release().await.unwrap();
        assert!(
            dataset
                .tags()
                .list()
                .await
                .unwrap()
                .keys()
                .all(|t| !t.starts_with(QUERY_TAG_PREFIX))
        );
        cleanup_old_versions(
            &dataset,
            CleanupPolicy {
                before_version: Some(v2),
                delete_unverified: true,
                error_if_tagged_old_versions: true,
                clean_referenced_branches: false,
                delete_rate_limit: None,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(dataset.checkout_version(v1).await.is_err());
    }

    /// Concurrent pins never collide on a tag name.
    #[tokio::test]
    async fn pins_get_distinct_tags() {
        let dir = TempDir::new().unwrap();
        let dataset =
            testing::write_dataset(&dir, &testing::vectors(4, 4), &["a", "b", "c", "d"]).await;
        let a = pin(&dataset).await.unwrap();
        let b = pin(&dataset).await.unwrap();
        assert_ne!(a.tag, b.tag);
        assert_eq!(dataset.tags().list().await.unwrap().len(), 2);
        a.release().await.unwrap();
        b.release().await.unwrap();
    }

    /// A stale lease tag left behind is reaped by the next pin; a stale-named
    /// tag this process still holds is not.
    #[tokio::test]
    async fn pin_reaps_abandoned_leases() {
        let dir = TempDir::new().unwrap();
        let dataset =
            testing::write_dataset(&dir, &testing::vectors(4, 4), &["a", "b", "c", "d"]).await;
        let stale = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            - LEASE_TTL.as_nanos()
            - 1;
        dataset
            .tags()
            .create(
                &format!("{QUERY_TAG_PREFIX}{}-{stale}-deadbeef", &*INSTANCE),
                Ref::Version(None, Some(dataset.version().version)),
            )
            .await
            .unwrap();
        let held = format!("{QUERY_TAG_PREFIX}{}-{stale}-livebeef", &*INSTANCE);
        dataset
            .tags()
            .create(&held, Ref::Version(None, Some(dataset.version().version)))
            .await
            .unwrap();
        LIVE_TAGS.lock().unwrap().insert(held.clone());

        let lease = pin(&dataset).await.unwrap();
        let tags = dataset.tags().list().await.unwrap();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains_key(&held));
        assert!(tags.contains_key(&lease.tag));
        LIVE_TAGS.lock().unwrap().remove(&held);
        lease.release().await.unwrap();
    }

    /// A stale lease from another process is not reaped at the short TTL —
    /// only the long foreign backstop applies.
    #[tokio::test]
    async fn foreign_leases_survive_local_reap() {
        let dir = TempDir::new().unwrap();
        let dataset =
            testing::write_dataset(&dir, &testing::vectors(4, 4), &["a", "b", "c", "d"]).await;
        let stale = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            - LEASE_TTL.as_nanos()
            - 1;
        let foreign = format!("{QUERY_TAG_PREFIX}other-{stale}-foreign");
        dataset
            .tags()
            .create(&foreign, Ref::Version(None, Some(dataset.version().version)))
            .await
            .unwrap();
        let lease = pin(&dataset).await.unwrap();
        let tags = dataset.tags().list().await.unwrap();
        assert!(tags.contains_key(&foreign), "foreign lease was reaped");
        lease.release().await.unwrap();
    }

    /// An all-digit foreign owner still gets the backstop TTL applied to its
    /// own timestamp rather than being read as a far-future legacy stamp.
    #[tokio::test]
    async fn all_digit_foreign_owner_still_expires() {
        let dir = TempDir::new().unwrap();
        let dataset =
            testing::write_dataset(&dir, &testing::vectors(4, 4), &["a", "b", "c", "d"]).await;
        let stale = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            - FOREIGN_TTL.as_nanos()
            - 1;
        let tag = format!("{QUERY_TAG_PREFIX}12345678901234567890123456789012-{stale}-deadbeef");
        dataset
            .tags()
            .create(&tag, Ref::Version(None, Some(dataset.version().version)))
            .await
            .unwrap();
        let lease = pin(&dataset).await.unwrap();
        let tags = dataset.tags().list().await.unwrap();
        assert!(!tags.contains_key(&tag), "all-digit foreign owner was kept");
        lease.release().await.unwrap();
    }

    /// Pins within one reap interval do not rescan the dataset's tags.
    #[tokio::test]
    async fn pin_throttles_reap_scans() {
        let dir = TempDir::new().unwrap();
        let dataset =
            testing::write_dataset(&dir, &testing::vectors(4, 4), &["a", "b", "c", "d"]).await;
        let first = pin(&dataset).await.unwrap();
        let stale = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            - LEASE_TTL.as_nanos()
            - 1;
        let tag = format!("{QUERY_TAG_PREFIX}{}-{stale}-slowbeef", &*INSTANCE);
        dataset
            .tags()
            .create(&tag, Ref::Version(None, Some(dataset.version().version)))
            .await
            .unwrap();

        let second = pin(&dataset).await.unwrap();
        assert!(dataset.tags().list().await.unwrap().contains_key(&tag));
        first.release().await.unwrap();
        second.release().await.unwrap();
    }

    /// A version reclaimed before its lease lands fails the pin instead of
    /// dying mid-query, and leaves no lease tag behind.
    #[tokio::test]
    async fn pin_fails_when_version_already_reclaimed() {
        let dir = TempDir::new().unwrap();
        let mut dataset =
            testing::write_dataset(&dir, &testing::vectors(4, 4), &["a", "b", "c", "d"]).await;
        let v1 = dataset.version().version;
        let old = dataset.checkout_version(v1).await.unwrap();
        testing::append_dataset(&mut dataset, &testing::vectors(4, 4), &["e", "f", "g", "h"]).await;
        let v2 = dataset.version().version;
        cleanup_old_versions(
            &dataset,
            CleanupPolicy {
                before_version: Some(v2),
                delete_unverified: true,
                error_if_tagged_old_versions: false,
                clean_referenced_branches: false,
                delete_rate_limit: None,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(dataset.checkout_version(v1).await.is_err());

        assert!(pin(&old).await.is_err());
        assert!(
            dataset
                .tags()
                .list()
                .await
                .unwrap()
                .keys()
                .all(|t| !t.starts_with(QUERY_TAG_PREFIX))
        );
    }

    /// A data file deleted under a pinned query fails the request rather than
    /// returning a partial result set.
    #[tokio::test]
    async fn missing_data_file_errors_instead_of_partial() {
        let dir = TempDir::new().unwrap();
        let dataset =
            testing::write_dataset(&dir, &testing::vectors(4, 4), &["a", "b", "c", "d"]).await;
        let data_dir = dir.path().join("data");
        let file = std::fs::read_dir(&data_dir)
            .unwrap()
            .flatten()
            .find(|e| e.file_name().to_string_lossy().ends_with(".lance"))
            .unwrap();
        std::fs::remove_file(file.path()).unwrap();

        let resolved = VectorTable::resolve(&dataset, None, None).unwrap();
        let q = Query {
            vector: vec![0.0; 4],
            k: 2,
            filter: None,
            postfilter: false,
            distance: DistanceType::L2,
            nprobes: None,
            index_segments: None,
            fragments: None,
        };
        assert!(query::flat(&dataset, &resolved, &q).await.is_err());
    }
}
