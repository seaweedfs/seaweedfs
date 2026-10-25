//! Issue #791: the shared `RwLock<Store>` guards the volume map only.
//! `find_volume` hands out an `Arc<Volume>` so needle I/O runs without the
//! guard; a handle that outlives its mount must fail closed rather than I/O
//! a torn-down volume — the same end Go reaches through nil needle-map/dat
//! checks under the per-volume dataFileAccessLock.

use std::sync::{Arc, RwLock};

use seaweed_volume::config::MinFreeSpace;
use seaweed_volume::storage::needle::needle::Needle;
use seaweed_volume::storage::needle_map::NeedleMapKind;
use seaweed_volume::storage::store::Store;
use seaweed_volume::storage::types::{DiskType, NeedleId, VolumeId};
use seaweed_volume::storage::volume::VolumeSpec;

use tempfile::TempDir;

fn store_with_two_volumes() -> (Arc<RwLock<Store>>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let mut store = Store::new(NeedleMapKind::InMemory);
    store
        .add_location(dir, dir, 10, DiskType::HardDrive, MinFreeSpace::Percent(1.0), Vec::new())
        .unwrap();
    store
        .add_volume(VolumeId(1), DiskType::HardDrive, &VolumeSpec::default())
        .unwrap();
    store
        .add_volume(VolumeId(2), DiskType::HardDrive, &VolumeSpec::default())
        .unwrap();
    (Arc::new(RwLock::new(store)), tmp)
}

fn needle(id: u64) -> Needle {
    Needle {
        id: NeedleId(id),
        cookie: 0x1234.into(),
        data: vec![7u8; 16],
        data_size: 16,
        ..Needle::default()
    }
}

#[test]
fn store_read_guard_admits_cross_volume_io() {
    let (store, _tmp) = store_with_two_volumes();

    // Read-guarded callers can write on different volumes concurrently —
    // what used to require store.write() now only resolves a handle.
    let t = {
        let store = store.clone();
        std::thread::spawn(move || {
            let store = store.read().unwrap();
            store
                .write_volume_needle(VolumeId(1), &mut needle(1), false)
                .unwrap();
        })
    };
    {
        let store = store.read().unwrap();
        store
            .write_volume_needle(VolumeId(2), &mut needle(9), false)
            .unwrap();
    }
    t.join().unwrap();

    let store = store.read().unwrap();
    let mut n = needle(9);
    n.data.clear();
    assert_eq!(store.read_volume_needle(VolumeId(2), &mut n).unwrap(), 16);
    let mut n = needle(1);
    n.data.clear();
    assert_eq!(store.read_volume_needle(VolumeId(1), &mut n).unwrap(), 16);
}

#[test]
fn stale_volume_handle_fails_after_unmount() {
    let (store, _tmp) = store_with_two_volumes();

    // A handle resolved before unmount keeps the Volume alive but must not
    // be able to write into its closed files.
    let (_, vol1) = store.read().unwrap().find_volume(VolumeId(1)).unwrap();
    store.write().unwrap().unmount_volume(VolumeId(1)).unwrap();

    assert!(vol1.write_needle(&mut needle(1), true, false).is_err());
    let mut n = needle(1);
    n.data.clear();
    assert!(vol1.read_needle(&mut n).is_err());
}
