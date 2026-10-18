package volume_replica

import (
	"fmt"
	"io"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// LiveNeedle is one live .idx NeedleId. Size is not part of the encode gate:
// replicas can differ by MIME/flags while still holding the same ids.
type LiveNeedle struct {
	Id types.NeedleId
}

type replicaLiveIdx struct {
	loc  wdclient.Location
	live []LiveNeedle
}

// LiveNeedlesFromIndex walks a compact needle map and returns live NeedleIds
// in key order. Deleted, Size<=0, and zero-offset rows are skipped.
func LiveNeedlesFromIndex(db *needle_map.MemDb) ([]LiveNeedle, error) {
	if db == nil {
		return nil, fmt.Errorf("nil needle index")
	}
	var live []LiveNeedle
	err := db.AscendingVisit(func(nv needle_map.NeedleValue) error {
		if nv.Offset.IsZero() || nv.Size.IsDeleted() || !nv.Size.IsValid() {
			return nil
		}
		live = append(live, LiveNeedle{Id: nv.Key})
		return nil
	})
	return live, err
}

// EqualLiveNeedleSets reports whether two live NeedleId sets match.
// Offset and Size are ignored.
func EqualLiveNeedleSets(a, b []LiveNeedle) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Id != b[i].Id {
			return false
		}
	}
	return true
}

// RequireCapacitySource returns loc if it is in capacityLocs (FreeVolumeCount>=2).
// An empty intersection refuses encode: do not fall back to a fat replica.
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

// SelectSourceIfIndexesMatch copies each replica's live .idx and compares
// NeedleId sets. A read error or a mismatch aborts: encode must not
// start and the union path must not run. One replica skips the comparison.
// On match, the source is late-bound to prefer (if present) or the first replica.
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
	for _, loc := range locations {
		status := GetReplicaStatus(grpcDialOption, vid, loc)
		if status.Error != nil {
			return wdclient.Location{}, fmt.Errorf("volume %d: replica status %s: %w", vid, loc.Url, status.Error)
		}
		db, err := ReadReplicaIndex(grpcDialOption, vid, collection, loc.ServerAddress())
		if err != nil {
			return wdclient.Location{}, fmt.Errorf("volume %d: read .idx from %s: %w", vid, loc.Url, err)
		}
		live, err := LiveNeedlesFromIndex(db)
		db.Close()
		if err != nil {
			return wdclient.Location{}, fmt.Errorf("volume %d: walk .idx from %s: %w", vid, loc.Url, err)
		}
		views = append(views, replicaLiveIdx{loc: loc, live: live})
	}

	return chooseSourceFromLiveNeedles(vid, views, prefer, writer)
}

func chooseSourceFromLiveNeedles(vid needle.VolumeId, views []replicaLiveIdx, prefer string, writer io.Writer) (wdclient.Location, error) {
	if len(views) == 0 {
		return wdclient.Location{}, fmt.Errorf("volume %d: no data-bearing replicas to encode", vid)
	}

	reference := views[0].live
	referenceUrl := views[0].loc.Url
	for _, view := range views[1:] {
		if !EqualLiveNeedleSets(reference, view.live) {
			return wdclient.Location{}, fmt.Errorf("volume %d: replica %s live NeedleId set does not match %s (%d vs %d entries); refusing encode (no union)",
				vid, view.loc.Url, referenceUrl, len(view.live), len(reference))
		}
	}

	if len(reference) == 0 {
		return wdclient.Location{}, fmt.Errorf("volume %d: no data-bearing replicas to encode", vid)
	}

	if writer != nil {
		fmt.Fprintf(writer, "volume %d: all %d data replicas are consistent (matching live idx sets, %d needles)\n",
			vid, len(views), len(reference))
	}

	if prefer != "" {
		for _, view := range views {
			if view.loc.Url == prefer {
				return view.loc, nil
			}
		}
	}
	return views[0].loc, nil
}

// ReadReplicaIndex streams a replica's .idx into an in-memory compact map.
// Any RPC or stream error is returned; the caller must treat it as "not equal".
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
