package shell

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// VolumeReplicaStatus represents the status of a volume replica
type VolumeReplicaStatus struct {
	Location         wdclient.Location
	FileCount        uint64
	FileDeletedCount uint64
	VolumeSize       uint64
	IsReadOnly       bool
	Error            error
}

// getVolumeReplicaStatus retrieves the current status of a volume replica
func getVolumeReplicaStatus(grpcDialOption grpc.DialOption, vid needle.VolumeId, location wdclient.Location) VolumeReplicaStatus {
	status := VolumeReplicaStatus{
		Location: location,
	}

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

// getVolumeReplicaStatuses retrieves status for all replicas of a volume
func getVolumeReplicaStatuses(grpcDialOption grpc.DialOption, vid needle.VolumeId, locations []wdclient.Location) []VolumeReplicaStatus {
	statuses := make([]VolumeReplicaStatus, len(locations))
	for i, location := range locations {
		statuses[i] = getVolumeReplicaStatus(grpcDialOption, vid, location)
	}
	return statuses
}

// replicaUnionBuilder builds a union replica by copying missing entries from other replicas
type replicaUnionBuilder struct {
	grpcDialOption grpc.DialOption
	writer         io.Writer
	vid            needle.VolumeId
	collection     string
}

// buildUnionReplica finds the largest replica and copies missing entries from other replicas into it.
// If excludeFromSelection is non-empty, that server won't be selected as the target but will still
// be used as a source for missing entries.
// Returns the location of the union replica (the one that now has all entries).
func (rub *replicaUnionBuilder) buildUnionReplica(locations []wdclient.Location, excludeFromSelection string) (wdclient.Location, int, error) {
	if len(locations) == 0 {
		return wdclient.Location{}, 0, fmt.Errorf("no replicas available")
	}
	if len(locations) == 1 {
		if locations[0].Url == excludeFromSelection {
			return wdclient.Location{}, 0, fmt.Errorf("only replica is excluded")
		}
		return locations[0], 0, nil
	}

	// Step 1: Find the largest replica (highest file count) that's not excluded
	statuses := getVolumeReplicaStatuses(rub.grpcDialOption, rub.vid, locations)

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

		// Read this replica's index
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

		// Find entries in other that are missing from best
		var missingNeedles []needle_map.NeedleValue
		doCutoffCheck := true

		otherDB.AscendingVisit(func(nv needle_map.NeedleValue) error {
			if nv.Size.IsDeleted() {
				return nil
			}
			if _, found := bestDB.Get(nv.Key); !found {
				// Check if too recent
				if doCutoffCheck {
					if needleMeta, err := readNeedleMeta(rub.grpcDialOption, loc.ServerAddress(), uint32(rub.vid), nv); err == nil {
						if needleMeta.AppendAtNs > cutoffFromAtNs {
							return nil
						}
						doCutoffCheck = false
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

		// Copy missing entries from this replica to best replica
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

			// Also add to bestDB so we don't copy duplicates from other replicas
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

func (rub *replicaUnionBuilder) readIndexDatabase(db *needle_map.MemDb, server pb.ServerAddress) error {
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

func (rub *replicaUnionBuilder) readNeedleBlob(server pb.ServerAddress, nv needle_map.NeedleValue) ([]byte, error) {
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

func (rub *replicaUnionBuilder) writeNeedleBlob(server pb.ServerAddress, nv needle_map.NeedleValue, needleBlob []byte) error {
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

// syncAndSelectBestReplica finds the largest replica, copies missing entries from other replicas
// into it to create a union, then returns this union replica for the operation.
// If excludeFromSelection is non-empty, that server won't be selected but will still contribute entries.
//
// The process:
// 1. Find the replica with the highest file count (the "best" one), excluding excludeFromSelection
// 2. For each other replica, find entries missing from best and copy them to best
// 3. Return the best replica which now contains the union of all entries
func syncAndSelectBestReplica(grpcDialOption grpc.DialOption, vid needle.VolumeId, collection string, locations []wdclient.Location, excludeFromSelection string, writer io.Writer) (wdclient.Location, error) {
	if len(locations) == 0 {
		return wdclient.Location{}, fmt.Errorf("no replicas available for volume %d", vid)
	}

	// Filter for checking consistency (exclude the excluded server)
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

	// Check if replicas are already consistent (skip sync if so)
	statuses := getVolumeReplicaStatuses(grpcDialOption, vid, locations)
	var validStatuses []VolumeReplicaStatus
	for i, s := range statuses {
		if s.Error == nil {
			// Include all for consistency check
			validStatuses = append(validStatuses, s)
			_ = i
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
			// All replicas are consistent, return the best non-excluded one
			for _, s := range validStatuses {
				if s.Location.Url != excludeFromSelection {
					fmt.Fprintf(writer, "volume %d: all %d replicas are consistent (file count: %d)\n",
						vid, len(validStatuses), s.FileCount)
					return s.Location, nil
				}
			}
		}
	}

	// Replicas are inconsistent, build union on the best replica
	fmt.Fprintf(writer, "volume %d: replicas are inconsistent, building union...\n", vid)

	builder := &replicaUnionBuilder{
		grpcDialOption: grpcDialOption,
		writer:         writer,
		vid:            vid,
		collection:     collection,
	}

	unionLocation, totalSynced, err := builder.buildUnionReplica(locations, excludeFromSelection)
	if err != nil {
		return wdclient.Location{}, fmt.Errorf("failed to build union replica: %w", err)
	}

	if totalSynced > 0 {
		fmt.Fprintf(writer, "volume %d: added %d entries to union replica %s\n", vid, totalSynced, unionLocation.Url)
	}

	return unionLocation, nil
}
