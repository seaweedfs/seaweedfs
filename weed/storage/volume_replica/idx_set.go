package volume_replica

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

type replicaLiveIdx struct {
	loc    wdclient.Location
	live   []types.NeedleId
	missed []types.NeedleId
}

// LiveNeedlesFromIndex walks a compact needle map and returns live NeedleIds
// in key order. Deleted, invalid-size, and zero-offset rows are skipped; Size
// is not part of the comparison since replicas can differ by MIME/flags while
// holding the same ids.
func LiveNeedlesFromIndex(db *needle_map.MemDb) ([]types.NeedleId, error) {
	if db == nil {
		return nil, fmt.Errorf("nil needle index")
	}
	var live []types.NeedleId
	err := db.AscendingVisit(func(nv needle_map.NeedleValue) error {
		if nv.Offset.IsZero() || nv.Size.IsDeleted() || !nv.Size.IsValid() {
			return nil
		}
		live = append(live, nv.Key)
		return nil
	})
	return live, err
}

// EqualLiveNeedleSets reports whether two live NeedleId sets match.
func EqualLiveNeedleSets(a, b []types.NeedleId) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// RequireCapacitySource returns loc if it is in capacityLocs (FreeVolumeCount>=2).
func RequireCapacitySource(vid needle.VolumeId, loc wdclient.Location, capacityLocs []wdclient.Location) (wdclient.Location, error) {
	if loc.Url == "" {
		return wdclient.Location{}, fmt.Errorf("volume %d: no encode source after idx compare", vid)
	}
	for _, c := range capacityLocs {
		if c.Url == loc.Url {
			return loc, nil
		}
	}
	return wdclient.Location{}, fmt.Errorf("volume %d: selected source %s is not in the FreeVolumeCount>=2 replica set; refusing encode", vid, loc.Url)
}

// SelectSourceIfIndexesMatch copies each replica's .idx and compares live
// NeedleId sets. A needle written once on one replica that another replica
// wrote identically and then tombstoned is a delete the first replica missed:
// it is left out of the comparison, and the source must hold none of them. Any other
// mismatch or a read error aborts. One replica skips the comparison. On match,
// the source is prefer when eligible, else the first eligible replica.
func SelectSourceIfIndexesMatch(grpcDialOption grpc.DialOption, vid needle.VolumeId, collection string, locations []wdclient.Location, prefer string, writer io.Writer) (wdclient.Location, error) {
	if len(locations) == 0 {
		return wdclient.Location{}, fmt.Errorf("no replicas available for volume %d", vid)
	}
	if len(locations) == 1 {
		if writer != nil {
			fmt.Fprintf(writer, "volume %d: single replica %s, skipping idx set compare\n", vid, locations[0].Url)
		}
		return locations[0], nil
	}

	views := make([]replicaLiveIdx, 0, len(locations))
	var latest, replaced []*needle_map.MemDb
	defer func() {
		for _, db := range append(latest, replaced...) {
			db.Close()
		}
	}()
	for _, loc := range locations {
		status := GetReplicaStatus(grpcDialOption, vid, loc)
		if status.Error != nil {
			return wdclient.Location{}, fmt.Errorf("volume %d: replica status %s: %w", vid, loc.Url, status.Error)
		}
		data, err := copyReplicaIndex(grpcDialOption, vid, collection, loc.ServerAddress())
		if err != nil {
			return wdclient.Location{}, fmt.Errorf("volume %d: read .idx from %s: %w", vid, loc.Url, err)
		}
		db, earlier := needle_map.NewMemDb(), needle_map.NewMemDb()
		if db == nil || earlier == nil {
			return wdclient.Location{}, fmt.Errorf("failed to allocate in-memory needle DB")
		}
		latest, replaced = append(latest, db), append(replaced, earlier)
		if err := loadIndexHistory(data, db, earlier); err != nil {
			return wdclient.Location{}, fmt.Errorf("volume %d: load .idx from %s: %w", vid, loc.Url, err)
		}
		live, err := LiveNeedlesFromIndex(db)
		if err != nil {
			return wdclient.Location{}, fmt.Errorf("volume %d: walk .idx from %s: %w", vid, loc.Url, err)
		}
		views = append(views, replicaLiveIdx{loc: loc, live: live})
	}

	lives := make([][]types.NeedleId, len(views))
	for i := range views {
		lives[i] = views[i].live
	}
	missed, err := MissedDeletes(lives, latest, replaced, func(i int, nv needle_map.NeedleValue) (*volume_server_pb.ReadNeedleMetaResponse, error) {
		meta, err := ReadNeedleMeta(grpcDialOption, locations[i].ServerAddress(), uint32(vid), nv)
		if err != nil {
			return nil, fmt.Errorf("volume %d: read needle %s meta from %s: %w", vid, nv.Key, locations[i].Url, err)
		}
		return meta, nil
	})
	if err != nil {
		return wdclient.Location{}, err
	}
	for i := range views {
		views[i].missed = missed[i]
	}
	return chooseSourceFromLiveNeedles(vid, views, prefer, writer)
}

// loadIndexHistory loads .idx rows into latest as ReadReplicaIndex does, and
// records in replaced, per key, the last live row that a later row replaced.
func loadIndexHistory(data []byte, latest, replaced *needle_map.MemDb) error {
	return idx.WalkIndexFile(bytes.NewReader(data), 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		if prev, found := latest.Get(key); found && !prev.Size.IsDeleted() && prev.Offset != offset {
			if err := replaced.Set(key, prev.Offset, prev.Size); err != nil {
				return err
			}
		}
		if offset.IsZero() {
			return latest.Delete(key)
		}
		return latest.Set(key, offset, size)
	})
}

// MissedDeletes returns, per replica, the needles live there that another
// replica holds as the same write (cookie, CRC, last-modified) followed by a
// tombstone: deletes the replica missed. live holds each replica's sorted live
// ids, latest its index with tombstone rows kept, replaced the last live row a
// later row replaced, and readMeta reads a row's record on replica i. The proof
// comes from the replicas' own indexes. A replica whose index has an earlier
// row for the needle (an overwrite or its own delete) may hold a rewrite, so it
// never counts. Compaction drops such rows, so the live write must also have
// been appended within replicatedWriteSpread of the deleted write, and before
// the tombstone.
func MissedDeletes(live [][]types.NeedleId, latest, replaced []*needle_map.MemDb, readMeta func(replica int, nv needle_map.NeedleValue) (*volume_server_pb.ReadNeedleMetaResponse, error)) ([][]types.NeedleId, error) {
	missed := make([][]types.NeedleId, len(live))
	for i := range live {
		notLiveSomewhere := make(map[types.NeedleId]bool)
		for j := range live {
			if j != i {
				for _, id := range subtractNeedleIds(live[i], live[j]) {
					notLiveSomewhere[id] = true
				}
			}
		}
		if len(notLiveSomewhere) == 0 {
			continue
		}
		for _, id := range live[i] {
			if !notLiveSomewhere[id] {
				continue
			}
			if _, rewritten := replaced[i].Get(id); rewritten {
				continue
			}
			liveRow, found := latest[i].Get(id)
			if !found {
				continue
			}
			var liveMeta *volume_server_pb.ReadNeedleMetaResponse
			for j := range live {
				if j == i {
					continue
				}
				tombstoneRow, found := latest[j].Get(id)
				if !found || !tombstoneRow.Size.IsDeleted() {
					continue
				}
				deletedWrite, found := replaced[j].Get(id)
				if !found {
					continue
				}
				if liveMeta == nil {
					meta, err := readMeta(i, *liveRow)
					if err != nil {
						return nil, err
					}
					liveMeta = meta
				}
				deletedMeta, err := readMeta(j, *deletedWrite)
				if err != nil {
					return nil, err
				}
				if liveMeta.GetCookie() != deletedMeta.GetCookie() || liveMeta.GetCrc() != deletedMeta.GetCrc() ||
					liveMeta.GetLastModified() != deletedMeta.GetLastModified() {
					continue
				}
				if !appendedTogether(liveMeta.GetAppendAtNs(), deletedMeta.GetAppendAtNs()) {
					continue
				}
				tombstoneMeta, err := readMeta(j, *tombstoneRow)
				if err != nil {
					return nil, err
				}
				if liveMeta.GetAppendAtNs() < tombstoneMeta.GetAppendAtNs() {
					missed[i] = append(missed[i], id)
					break
				}
			}
		}
	}
	return missed, nil
}

// replicatedWriteSpread bounds how far apart two replicas stamp one replicated
// write: the forwarding time plus their clock skew.
const replicatedWriteSpread = time.Minute

// appendedTogether reports whether two records were appended within
// replicatedWriteSpread of each other, as one replicated write is.
func appendedTogether(a, b uint64) bool {
	if a < b {
		a, b = b, a
	}
	return a-b <= uint64(replicatedWriteSpread)
}

// subtractNeedleIds returns the ids of sorted a that are not in sorted b.
func subtractNeedleIds(a, b []types.NeedleId) []types.NeedleId {
	if len(b) == 0 {
		return a
	}
	var out []types.NeedleId
	j := 0
	for _, id := range a {
		for j < len(b) && b[j] < id {
			j++
		}
		if j < len(b) && b[j] == id {
			continue
		}
		out = append(out, id)
	}
	return out
}

func chooseSourceFromLiveNeedles(vid needle.VolumeId, views []replicaLiveIdx, prefer string, writer io.Writer) (wdclient.Location, error) {
	if len(views) == 0 {
		return wdclient.Location{}, fmt.Errorf("volume %d: no data-bearing replicas to encode", vid)
	}

	reference := subtractNeedleIds(views[0].live, views[0].missed)
	referenceUrl := views[0].loc.Url
	for _, view := range views[1:] {
		agreed := subtractNeedleIds(view.live, view.missed)
		if !EqualLiveNeedleSets(reference, agreed) {
			return wdclient.Location{}, fmt.Errorf("volume %d: replica %s live NeedleId set does not match %s (%d vs %d entries); refusing encode (no union)",
				vid, view.loc.Url, referenceUrl, len(agreed), len(reference))
		}
	}

	if len(reference) == 0 {
		return wdclient.Location{}, fmt.Errorf("volume %d: no data-bearing replicas to encode", vid)
	}

	var source *replicaLiveIdx
	missedDeletes := 0
	for i := range views {
		if len(views[i].missed) > 0 {
			missedDeletes += len(views[i].missed)
			if writer != nil {
				fmt.Fprintf(writer, "volume %d: replica %s missed %d delete(s) that another replica applied later; not encoding from it\n",
					vid, views[i].loc.Url, len(views[i].missed))
			}
			continue
		}
		if source == nil || views[i].loc.Url == prefer {
			source = &views[i]
		}
	}
	if source == nil {
		return wdclient.Location{}, fmt.Errorf("volume %d: every replica missed a delete another replica applied; refusing encode (no union), run volume.check.disk -volumeId %d -slow -syncDeleted -apply first", vid, vid)
	}

	if writer != nil {
		if missedDeletes == 0 {
			fmt.Fprintf(writer, "volume %d: all %d data replicas are consistent (matching live idx sets, %d needles)\n",
				vid, len(views), len(reference))
		} else {
			fmt.Fprintf(writer, "volume %d: all %d data replicas are consistent apart from %d missed delete(s) (%d needles); encoding from %s\n",
				vid, len(views), missedDeletes, len(reference), source.loc.Url)
		}
	}
	return source.loc, nil
}

// ReadReplicaIndex streams a replica's .idx into an in-memory compact map.
func ReadReplicaIndex(grpcDialOption grpc.DialOption, vid needle.VolumeId, collection string, server pb.ServerAddress) (*needle_map.MemDb, error) {
	db := needle_map.NewMemDb()
	if db == nil {
		return nil, fmt.Errorf("failed to allocate in-memory needle DB")
	}
	if err := loadReplicaIndex(db, grpcDialOption, vid, collection, server); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
