package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandVacuum{})
}

type commandVacuum struct {
}

func (c *commandVacuum) Name() string {
	return "volume.vacuum"
}

func (c *commandVacuum) Help() string {
	return `compact volumes if deleted entries are more than the limit

	volume.vacuum [-garbageThreshold=0.3] [-collection=<collection name>] [-volumeId=<volume id>]

	Without -volumeId this runs the same sweep as the automatic vacuum, which skips
	read-only volumes. Name a read-only volume with -volumeId to vacuum it anyway.

`
}

func (c *commandVacuum) HasTag(CommandTag) bool {
	return false
}

func (c *commandVacuum) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volumeVacuumCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	garbageThreshold := volumeVacuumCommand.Float64("garbageThreshold", 0.3, "vacuum when garbage is more than this limit")
	collection := volumeVacuumCommand.String("collection", "", "comma-separated collection names, wildcards, or regex patterns; empty or \"*\" matches all, \""+CollectionDefault+"\" the collection with no name")
	volumeIds := volumeVacuumCommand.String("volumeId", "", "comma-separated list of volume IDs")
	if err = volumeVacuumCommand.Parse(args); err != nil {
		return nil
	}

	collectionMatcher, err := wildcard.CompileCollectionMatcher(*collection)
	if err != nil {
		return fmt.Errorf("invalid collection pattern %q: %w", *collection, err)
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	var volumeIdInts []uint32
	if *volumeIds != "" {
		for _, volumeIdStr := range strings.Split(*volumeIds, ",") {
			volumeIdStr = strings.TrimSpace(volumeIdStr)
			if volumeIdInt, err := strconv.ParseUint(volumeIdStr, 10, 32); err == nil {
				volumeIdInts = append(volumeIdInts, uint32(volumeIdInt))
			} else {
				return fmt.Errorf("parse volumeId string %s to int: %v", volumeIdStr, err)
			}
		}
	} else {
		volumeIdInts = append(volumeIdInts, 0)
	}

	topo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		if *volumeIds != "" {
			return fmt.Errorf("collect topology: %w", err)
		}
		// The hint below is a courtesy; a sweep must not depend on it.
		fmt.Fprintf(writer, "could not list volumes to check for read-only ones: %v\n", err)
	}

	// Reject unknown ids up front. The master's VacuumVolume RPC silently
	// iterates matching volumes, so a typo or an already-deleted volume just
	// returns success — making this command look like it worked when nothing
	// happened.
	if *volumeIds != "" {
		known := make(map[uint32]bool)
		eachDataNode(topo, func(_ DataCenterId, _ RackId, dn *master_pb.DataNodeInfo) {
			for _, disk := range dn.DiskInfos {
				for _, vi := range disk.VolumeInfos {
					known[vi.Id] = true
				}
			}
		})
		// Dedupe via a set so "volume.vacuum -volumeId 5,5,5" on a missing
		// volume 5 reports [5] once instead of [5 5 5].
		missingSet := make(map[uint32]bool)
		for _, vid := range volumeIdInts {
			if !known[vid] {
				missingSet[vid] = true
			}
		}
		if len(missingSet) > 0 {
			missing := make([]uint32, 0, len(missingSet))
			for vid := range missingSet {
				missing = append(missing, vid)
			}
			sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
			return fmt.Errorf("volume(s) not found on master: %v", missing)
		}
	} else if topo != nil {
		// The sweep says nothing about the volumes it leaves alone, so an
		// operator on a full disk sees the command return and nothing change.
		if skipped := readOnlyVolumesAboveThreshold(topo, collectionMatcher, *garbageThreshold); len(skipped) > 0 {
			fmt.Fprintf(writer, "%d read-only volume(s) hold garbage above %g and are skipped by the sweep: %v\n", len(skipped), *garbageThreshold, skipped)
			fmt.Fprintf(writer, "vacuum them explicitly with -volumeId\n")
		}
	}

	// The VacuumVolume RPC takes one exact collection name per call and reads
	// "" as "all collections", so a filter has to be resolved here: one request
	// per matched named collection, and per-volume requests for the unnamed
	// collection, which no Collection value selects on its own.
	var requests []*master_pb.VacuumVolumeRequest
	if collectionMatcher == nil {
		for _, volumeId := range volumeIdInts {
			requests = append(requests, &master_pb.VacuumVolumeRequest{
				VolumeId: volumeId,
			})
		}
	} else {
		if topo == nil {
			return fmt.Errorf("cannot resolve collection pattern %q: topology unavailable", *collection)
		}
		if *volumeIds != "" {
			requests, err = vacuumRequestsForVolumeIds(topo, collectionMatcher, volumeIdInts)
		} else {
			requests, err = vacuumRequestsForPattern(commandEnv.option.GrpcDialOption, writer, topo, collectionMatcher)
		}
		if err != nil {
			return err
		}
	}

	for _, req := range requests {
		req.GarbageThreshold = float32(*garbageThreshold)
		err = commandEnv.MasterClient.WithClient(context.Background(), false, func(client master_pb.SeaweedClient) error {
			_, err = client.VacuumVolume(context.Background(), req)
			return err
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// vidReplica pairs a volume replica's read-only state with the server that
// reports it, so the unnamed-collection sweep can reproduce the master's
// low-disk-space exception client-side.
type vidReplica struct {
	server   pb.ServerAddress
	readOnly bool
}

// vacuumRequestsForPattern resolves a collection filter to the sweep requests
// that cover it: one per matched named collection, which keeps the server's
// read-only skip, plus per-volume requests for the unnamed collection — no
// Collection value scopes the RPC to it alone.
func vacuumRequestsForPattern(grpcDialOption grpc.DialOption, writer io.Writer, topo *master_pb.TopologyInfo, collectionMatcher *wildcard.CollectionMatcher) ([]*master_pb.VacuumVolumeRequest, error) {
	collections := make(map[string]bool)
	defaultReplicas := make(map[uint32][]vidReplica)
	eachDataNode(topo, func(_ DataCenterId, _ RackId, dn *master_pb.DataNodeInfo) {
		for _, disk := range dn.DiskInfos {
			for _, v := range disk.VolumeInfos {
				if !collectionMatcher.Matches(v.Collection) {
					continue
				}
				collections[v.Collection] = true
				if v.Collection == "" {
					defaultReplicas[v.Id] = append(defaultReplicas[v.Id], vidReplica{
						server:   pb.NewServerAddressFromDataNode(dn),
						readOnly: v.ReadOnly,
					})
				}
			}
		}
	})
	if len(collections) == 0 {
		return nil, fmt.Errorf("no volumes found in collections matching the filter")
	}

	var requests []*master_pb.VacuumVolumeRequest
	for name := range collections {
		if name != "" {
			requests = append(requests, &master_pb.VacuumVolumeRequest{Collection: name})
		}
	}
	// The unnamed collection has no scoping name, so each of its volumes gets
	// a per-volume request — which also vacuums any other collection sharing
	// the id. A volume whose every replica is read-only is skipped unless a
	// replica reports low disk space, the sweep's own exception.
	var defaultVids []uint32
	for vid, replicas := range defaultReplicas {
		writable := false
		for _, r := range replicas {
			if !r.readOnly {
				writable = true
				break
			}
		}
		if !writable && !anyReplicaDiskSpaceLow(grpcDialOption, replicas, vid, writer) {
			continue
		}
		defaultVids = append(defaultVids, vid)
	}
	sort.Slice(defaultVids, func(i, j int) bool { return defaultVids[i] < defaultVids[j] })
	for _, vid := range defaultVids {
		requests = append(requests, &master_pb.VacuumVolumeRequest{VolumeId: vid})
	}
	return requests, nil
}

// anyReplicaDiskSpaceLow probes read-only replicas for the low-disk condition
// under which the master's sweep still vacuums a read-only volume.
func anyReplicaDiskSpaceLow(grpcDialOption grpc.DialOption, replicas []vidReplica, vid uint32, writer io.Writer) bool {
	for _, r := range replicas {
		diskSpaceLow := false
		err := operation.WithVolumeServerClient(false, r.server, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resp, checkErr := client.VacuumVolumeCheck(ctx, &volume_server_pb.VacuumVolumeCheckRequest{VolumeId: vid})
			if checkErr != nil {
				return checkErr
			}
			diskSpaceLow = resp.DiskSpaceLow
			return nil
		})
		if err != nil {
			fmt.Fprintf(writer, "volume %d on %s: cannot check disk space: %v\n", vid, r.server, err)
			continue
		}
		if diskSpaceLow {
			return true
		}
	}
	return false
}

// vacuumRequestsForVolumeIds scopes each requested volume to the collections
// it lives in that the filter matches, so -volumeId and a pattern narrow each
// other the same way the server narrows an exact -collection name.
func vacuumRequestsForVolumeIds(topo *master_pb.TopologyInfo, collectionMatcher *wildcard.CollectionMatcher, volumeIdInts []uint32) ([]*master_pb.VacuumVolumeRequest, error) {
	collectionsByVid := make(map[uint32]map[string]bool)
	eachDataNode(topo, func(_ DataCenterId, _ RackId, dn *master_pb.DataNodeInfo) {
		for _, disk := range dn.DiskInfos {
			for _, v := range disk.VolumeInfos {
				if collectionsByVid[v.Id] == nil {
					collectionsByVid[v.Id] = make(map[string]bool)
				}
				collectionsByVid[v.Id][v.Collection] = true
			}
		}
	})

	var requests []*master_pb.VacuumVolumeRequest
	for _, vid := range volumeIdInts {
		var matched []string
		for name := range collectionsByVid[vid] {
			if collectionMatcher.Matches(name) {
				matched = append(matched, name)
			}
		}
		if len(matched) == 0 {
			return nil, fmt.Errorf("volume %d is not in a collection matching the filter", vid)
		}
		// The unnamed collection has no scoping name; a volume-id-only request
		// covers it (and any other collection the id is shared into).
		if slices.Contains(matched, "") {
			requests = append(requests, &master_pb.VacuumVolumeRequest{VolumeId: vid})
			continue
		}
		sort.Strings(matched)
		for _, name := range matched {
			requests = append(requests, &master_pb.VacuumVolumeRequest{VolumeId: vid, Collection: name})
		}
	}
	return requests, nil
}

// readOnlyVolumesAboveThreshold lists the volumes a sweep leaves alone: any
// replica read-only (that is what the sweep checks), in the collection when one
// is given, and some replica with a garbage ratio at or above the threshold.
// The ratio uses the sizes the master reports, which is deleted bytes over the
// .dat size rather than over the content size the volume server divides by, so
// it can only understate, and a converted index that reports deletes without
// their sizes is left out because its ratio is not knowable here. This is a
// hint; the volume server's own check decides.
func readOnlyVolumesAboveThreshold(topo *master_pb.TopologyInfo, collectionMatcher *wildcard.CollectionMatcher, garbageThreshold float64) []uint32 {
	readOnly := make(map[uint32]bool)
	garbage := make(map[uint32]float64) // the highest ratio any replica reports
	eachDataNode(topo, func(_ DataCenterId, _ RackId, dn *master_pb.DataNodeInfo) {
		for _, disk := range dn.DiskInfos {
			for _, v := range disk.VolumeInfos {
				if !collectionMatcher.Matches(v.Collection) {
					continue
				}
				if v.ReadOnly {
					readOnly[v.Id] = true
				}
				if v.Size == 0 {
					continue
				}
				if ratio := float64(v.DeletedByteCount) / float64(v.Size); ratio > garbage[v.Id] {
					garbage[v.Id] = ratio
				}
			}
		}
	})
	vids := make([]uint32, 0, len(readOnly))
	for vid := range readOnly {
		if garbage[vid] >= garbageThreshold {
			vids = append(vids, vid)
		}
	}
	sort.Slice(vids, func(i, j int) bool { return vids[i] < vids[j] })
	return vids
}
