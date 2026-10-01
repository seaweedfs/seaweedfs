//! Merging a peer's `.ecj` deletion ids into a local EC journal. Mirrors Go's
//! `Store.MergeEcJournal` (`weed/storage/store_ec_journal.go`).

use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::sync::RwLock;

use crate::storage::erasure_coding::ecj_merge::{append_ecj_ids, read_ecj_ids};
use crate::storage::store::Store;
use crate::storage::types::{NeedleId, VolumeId};

/// How often an unmounted merge re-reads a journal that changed under it. Only
/// a mount-delete-unmount or a concurrent merge between the read and the
/// append changes it, so one retry is nearly always enough.
const ECJ_MERGE_ATTEMPTS: usize = 5;

/// Fold a peer's deletion `ids` into the local journal of EC volume `vid`,
/// whose on-disk path on the receiving disk is `ecj_path`. Appends only the
/// ids the journal lacks and returns how many it added. Blocking: call it from
/// `spawn_blocking`.
///
/// A mounted volume owns its journal: the merge goes through its open handle
/// and in-memory set, wherever that journal lives (it may sit in the data dir
/// rather than `ecj_path`'s index dir). Otherwise `ecj_path` is appended to
/// under the store write lock, which mounts take, so no mount can open it
/// mid-append. The read that computes the delta runs outside the lock, and a
/// journal that changed in between — including by a concurrent merge — is
/// re-read.
pub fn merge_ec_journal(
    store: &RwLock<Store>,
    vid: VolumeId,
    ecj_path: &str,
    ids: &HashSet<NeedleId>,
) -> io::Result<usize> {
    let dir = Path::new(ecj_path).parent().unwrap_or(Path::new(""));
    for _ in 0..ECJ_MERGE_ATTEMPTS {
        {
            let mut store = store
                .write()
                .map_err(|_| io::Error::other("store lock poisoned"))?;
            if let Some(added) = merge_into_mounted(&mut store, vid, ecj_path, dir, ids)? {
                return Ok(added);
            }
        }
        let (local, size) = read_ecj_ids(ecj_path)?;
        let mut store = store
            .write()
            .map_err(|_| io::Error::other("store lock poisoned"))?;
        if merge_into_mounted(&mut store, vid, ecj_path, dir, ids)?.is_some() {
            // Mounted since the read; the next pass merges through it.
            continue;
        }
        if let Some(added) = append_ecj_ids(ecj_path, &local, ids, size)? {
            return Ok(added);
        }
    }
    Err(io::Error::other(format!(
        "ec volume {}: journal {} kept changing during merge",
        vid.0, ecj_path
    )))
}

/// Merge into the mounted volume that owns this journal, if any: the disk
/// owning `dir` holds vid's runtime for these shards, and reconciliation can
/// mount vid on a sibling disk that journals into `dir` (#9212). Errors when no
/// disk owns `dir` at all.
fn merge_into_mounted(
    store: &mut Store,
    vid: VolumeId,
    ecj_path: &str,
    dir: &Path,
    ids: &HashSet<NeedleId>,
) -> io::Result<Option<usize>> {
    let mut owner = None;
    for (i, loc) in store.locations.iter().enumerate() {
        if Path::new(&loc.idx_directory) == dir || Path::new(&loc.directory) == dir {
            owner = Some(i);
        } else if loc
            .find_ec_volume(vid)
            .is_some_and(|ecv| ecv.ecj_file_name() == ecj_path)
        {
            return store.locations[i]
                .find_ec_volume_mut(vid)
                .map(|ecv| ecv.merge_journal(ids))
                .transpose();
        }
    }
    let owner = owner.ok_or_else(|| {
        io::Error::other(format!(
            "ec volume {}: no disk owns journal {}",
            vid.0, ecj_path
        ))
    })?;
    store.locations[owner]
        .find_ec_volume_mut(vid)
        .map(|ecv| ecv.merge_journal(ids))
        .transpose()
}
