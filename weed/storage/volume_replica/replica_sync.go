// Package volume_replica reconciles regular (non-EC) volume replicas: it reads
// per-replica status, builds the union of all live entries onto the most-complete
// replica, and returns that replica. It is shared by the shell (volume.tier.move,
// ec.encode, replica check) and the EC encode worker so a stale replica is never
// used as the basis of an operation.
package volume_replica

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// ReplicaStatus is the observed state of one volume replica.
type ReplicaStatus struct {
	Location         wdclient.Location
	FileCount        uint64
	FileDeletedCount uint64
	VolumeSize       uint64
	IsReadOnly       bool
	Error            error
}

// GetReplicaStatus retrieves the current status of a single volume replica.
func GetReplicaStatus(grpcDialOption grpc.DialOption, vid needle.VolumeId, location wdclient.Location) ReplicaStatus {
	status := ReplicaStatus{Location: location}
	err := operation.WithVolumeServerClient(false, location.ServerAddress(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		resp, reqErr := volumeServerClient.VolumeStatus(context.Background(), &volume_server_pb.VolumeStatusRequest{
			VolumeId: uint32(vid),
		})
		if reqErr != nil {
			return reqErr
		}
		if resp != nil {
			status.FileCount = resp.FileCount
			status.FileDeletedCount = resp.FileDeletedCount
			status.VolumeSize = resp.VolumeSize
			status.IsReadOnly = resp.IsReadOnly
		}
		return nil
	})
	status.Error = err
	return status
}

// GetReplicaStatuses retrieves status for all replicas of a volume in parallel.
func GetReplicaStatuses(grpcDialOption grpc.DialOption, vid needle.VolumeId, locations []wdclient.Location) []ReplicaStatus {
	statuses := make([]ReplicaStatus, len(locations))
	var wg sync.WaitGroup
	for i, location := range locations {
		wg.Add(1)
		go func(i int, location wdclient.Location) {
			defer wg.Done()
			statuses[i] = GetReplicaStatus(grpcDialOption, vid, location)
		}(i, location)
	}
	wg.Wait()
	return statuses
}

// ReadNeedleMeta reads a needle's metadata (e.g. AppendAtNs) from a volume server.
func ReadNeedleMeta(grpcDialOption grpc.DialOption, volumeServer pb.ServerAddress, volumeId uint32, needleValue needle_map.NeedleValue) (resp *volume_server_pb.ReadNeedleMetaResponse, err error) {
	err = operation.WithVolumeServerClient(false, volumeServer, grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			if resp, err = client.ReadNeedleMeta(context.Background(), &volume_server_pb.ReadNeedleMetaRequest{
				VolumeId: volumeId,
				NeedleId: uint64(needleValue.Key),
				Offset:   needleValue.Offset.ToActualOffset(),
				Size:     int32(needleValue.Size),
			}); err != nil {
				return err
			}
			return nil
		},
	)
	return
}

// unionBuilder builds a union replica by copying missing entries from other replicas.
type unionBuilder struct {
	grpcDialOption grpc.DialOption
	writer         io.Writer
	vid            needle.VolumeId
	collection     string
}

// buildUnionReplica finds the largest replica and copies missing entries from other
// replicas into it. If excludeFromSelection is non-empty, that server won't be
// selected as the target but will still be used as a source for missing entries.
// Returns the location of the union replica (the one that now has all entries).
func (rub *unionBuilder) buildUnionReplica(locations []wdclient.Location, statuses []ReplicaStatus, excludeFromSelection string) (wdclient.Location, int, error) {
	if len(locations) == 0 {
		return wdclient.Location{}, 0, fmt.Errorf("no replicas available")
	}
	if len(locations) == 1 {
		if locations[0].Url == excludeFromSelection {
			return wdclient.Location{}, 0, fmt.Errorf("only replica is excluded")
		}
		return locations[0], 0, nil
	}

	// Step 1: Find the largest replica (highest file count) that's not excluded.
	// statuses are supplied by the caller to avoid a redundant status RPC sweep.
	bestIdx := -1
	var bestFileCount uint64
	for i, s := range statuses {
		if s.Error == nil && locations[i].Url != excludeFromSelection {
			if bestIdx == -1 || s.FileCount > bestFileCount {
				bestIdx = i
				bestFileCount = s.FileCount
			}
		}
	}

	if bestIdx == -1 {
		return wdclient.Location{}, 0, fmt.Errorf("could not find valid replica (all excluded or errored)")
	}

	bestLocation := locations[bestIdx]
	fmt.Fprintf(rub.writer, "volume %d: selected %s as best replica (file count: %d)\n",
		rub.vid, bestLocation.Url, bestFileCount)

	// Step 2: Read index database from the best replica
	bestDB := needle_map.NewMemDb()
	if bestDB == nil {
		return wdclient.Location{}, 0, fmt.Errorf("failed to allocate in-memory needle DB")
	}
	defer bestDB.Close()

	if err := rub.readIndexDatabase(bestDB, bestLocation.ServerAddress()); err != nil {
		return wdclient.Location{}, 0, fmt.Errorf("read index from best replica %s: %w", bestLocation.Url, err)
	}

	// Step 3: For each other replica (including excluded), find entries missing from best and copy them
	totalSynced := 0
	cutoffFromAtNs := uint64(time.Now().UnixNano())

	for i, loc := range locations {
		if i == bestIdx {
			continue
		}
		if statuses[i].Error != nil {
			fmt.Fprintf(rub.writer, "  skipping %s: %v\n", loc.Url, statuses[i].Error)
			continue
		}

		otherDB := needle_map.NewMemDb()
		if otherDB == nil {
			fmt.Fprintf(rub.writer, "  skipping %s: failed to allocate DB\n", loc.Url)
			continue
		}

		if err := rub.readIndexDatabase(otherDB, loc.ServerAddress()); err != nil {
			otherDB.Close()
			fmt.Fprintf(rub.writer, "  skipping %s: %v\n", loc.Url, err)
			continue
		}

		var missingNeedles []needle_map.NeedleValue
		otherDB.AscendingVisit(func(nv needle_map.NeedleValue) error {
			if nv.Size.IsDeleted() {
				return nil
			}
			if _, found := bestDB.Get(nv.Key); !found {
				// Skip entries written after sync started to avoid copying in-flight
				// writes. A read-only replica accepts no new writes, so the per-needle
				// metadata RPC is unnecessary then (ec.encode/tier.move mark readonly
				// before syncing).
				if !statuses[i].IsReadOnly {
					if needleMeta, err := ReadNeedleMeta(rub.grpcDialOption, loc.ServerAddress(), uint32(rub.vid), nv); err == nil {
						if needleMeta.AppendAtNs > cutoffFromAtNs {
							return nil
						}
					}
				}
				missingNeedles = append(missingNeedles, nv)
			}
			return nil
		})
		otherDB.Close()

		if len(missingNeedles) == 0 {
			continue
		}

		syncedFromThis := 0
		for _, nv := range missingNeedles {
			needleBlob, err := rub.readNeedleBlob(loc.ServerAddress(), nv)
			if err != nil {
				fmt.Fprintf(rub.writer, "  warning: read needle %d from %s: %v\n", nv.Key, loc.Url, err)
				continue
			}

			if err := rub.writeNeedleBlob(bestLocation.ServerAddress(), nv, needleBlob); err != nil {
				fmt.Fprintf(rub.writer, "  warning: write needle %d to %s: %v\n", nv.Key, bestLocation.Url, err)
				continue
			}

			bestDB.Set(nv.Key, nv.Offset, nv.Size)
			syncedFromThis++
		}

		if syncedFromThis > 0 {
			fmt.Fprintf(rub.writer, "  copied %d entries from %s to %s\n",
				syncedFromThis, loc.Url, bestLocation.Url)
			totalSynced += syncedFromThis
		}
	}

	return bestLocation, totalSynced, nil
}

func (rub *unionBuilder) readIndexDatabase(db *needle_map.MemDb, server pb.ServerAddress) error {
	var buf bytes.Buffer

	err := operation.WithVolumeServerClient(true, server, rub.grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		copyFileClient, err := volumeServerClient.CopyFile(context.Background(), &volume_server_pb.CopyFileRequest{
			VolumeId:                 uint32(rub.vid),
			Ext:                      ".idx",
			CompactionRevision:       math.MaxUint32,
			StopOffset:               math.MaxInt64,
			Collection:               rub.collection,
			IsEcVolume:               false,
			IgnoreSourceFileNotFound: false,
		})
		if err != nil {
			return fmt.Errorf("start copy: %w", err)
		}

		for {
			resp, recvErr := copyFileClient.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				return fmt.Errorf("receive: %w", recvErr)
			}
			buf.Write(resp.FileContent)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return db.LoadFilterFromReaderAt(bytes.NewReader(buf.Bytes()), true, false)
}

func (rub *unionBuilder) readNeedleBlob(server pb.ServerAddress, nv needle_map.NeedleValue) ([]byte, error) {
	var needleBlob []byte
	err := operation.WithVolumeServerClient(false, server, rub.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		resp, err := client.ReadNeedleBlob(context.Background(), &volume_server_pb.ReadNeedleBlobRequest{
			VolumeId: uint32(rub.vid),
			Offset:   nv.Offset.ToActualOffset(),
			Size:     int32(nv.Size),
		})
		if err != nil {
			return err
		}
		needleBlob = resp.NeedleBlob
		return nil
	})
	return needleBlob, err
}

func (rub *unionBuilder) writeNeedleBlob(server pb.ServerAddress, nv needle_map.NeedleValue, needleBlob []byte) error {
	return operation.WithVolumeServerClient(false, server, rub.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		_, err := client.WriteNeedleBlob(context.Background(), &volume_server_pb.WriteNeedleBlobRequest{
			VolumeId:   uint32(rub.vid),
			NeedleId:   uint64(nv.Key),
			Size:       int32(nv.Size),
			NeedleBlob: needleBlob,
		})
		return err
	})
}

// SyncAndSelectBestReplica finds the largest replica, copies missing entries from
// other replicas into it to create a union, then returns this union replica for the
// operation. If excludeFromSelection is non-empty, that server won't be selected
// but will still contribute entries. Already-consistent replica sets skip the sync.
func SyncAndSelectBestReplica(grpcDialOption grpc.DialOption, vid needle.VolumeId, collection string, locations []wdclient.Location, excludeFromSelection string, writer io.Writer) (wdclient.Location, error) {
	if len(locations) == 0 {
		return wdclient.Location{}, fmt.Errorf("no replicas available for volume %d", vid)
	}

	var checkLocations []wdclient.Location
	for _, loc := range locations {
		if loc.Url != excludeFromSelection {
			checkLocations = append(checkLocations, loc)
		}
	}

	if len(checkLocations) == 0 {
		return wdclient.Location{}, fmt.Errorf("no replicas available for volume %d after exclusion", vid)
	}

	if len(checkLocations) == 1 && len(locations) == 1 {
		return checkLocations[0], nil
	}

	// Check if replicas are already consistent (skip sync if so).
	statuses := GetReplicaStatuses(grpcDialOption, vid, locations)
	var validStatuses []ReplicaStatus
	for _, s := range statuses {
		if s.Error == nil {
			validStatuses = append(validStatuses, s)
		}
	}

	if len(validStatuses) > 1 {
		allSame := true
		for _, s := range validStatuses[1:] {
			if s.FileCount != validStatuses[0].FileCount {
				allSame = false
				break
			}
		}
		if allSame {
			for _, s := range validStatuses {
				if s.Location.Url != excludeFromSelection {
					fmt.Fprintf(writer, "volume %d: all %d replicas are consistent (file count: %d)\n",
						vid, len(validStatuses), s.FileCount)
					return s.Location, nil
				}
			}
		}
	}

	fmt.Fprintf(writer, "volume %d: replicas are inconsistent, building union...\n", vid)

	builder := &unionBuilder{
		grpcDialOption: grpcDialOption,
		writer:         writer,
		vid:            vid,
		collection:     collection,
	}

	unionLocation, totalSynced, err := builder.buildUnionReplica(locations, statuses, excludeFromSelection)
	if err != nil {
		return wdclient.Location{}, fmt.Errorf("failed to build union replica: %w", err)
	}

	if totalSynced > 0 {
		fmt.Fprintf(writer, "volume %d: added %d entries to union replica %s\n", vid, totalSynced, unionLocation.Url)
	}

	return unionLocation, nil
}
