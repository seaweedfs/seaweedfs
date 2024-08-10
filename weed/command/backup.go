package command

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/storage"
)

var (
	s BackupOptions
)

type BackupOptions struct {
	master      *string
	collection  *string
	dir         *string
	volumeId    *int
	ttl         *string
	replication *string
}

func init() {
	cmdBackup.Run = runBackup // break init cycle
	s.master = cmdBackup.Flag.String("server", "localhost:9333", "SeaweedFS master location")
	s.collection = cmdBackup.Flag.String("collection", "", "collection name")
	s.dir = cmdBackup.Flag.String("dir", ".", "directory to store volume data files")
	s.volumeId = cmdBackup.Flag.Int("volumeId", -1, "a volume id. The volume .dat and .idx files should already exist in the dir.")
	s.ttl = cmdBackup.Flag.String("ttl", "", `backup volume's time to live, format: 
				3m: 3 minutes
				4h: 4 hours
				5d: 5 days
				6w: 6 weeks
				7M: 7 months
				8y: 8 years
				default is the same with origin`)
	s.replication = cmdBackup.Flag.String("replication", "", "backup volume's replication, default is the same with origin")
}

var cmdBackup = &Command{
	UsageLine: "backup -dir=. -volumeId=234 -server=localhost:9333",
	Short:     "incrementally backup a volume to local folder",
	Long: `Incrementally backup volume data.

	It is expected that you use this inside a script, to loop through
	all possible volume ids that needs to be backup to local folder.

	The volume id does not need to exist locally or even remotely.
	This will help to backup future new volumes.

	Usually backing up is just copying the .dat (and .idx) files.
	But it's tricky to incrementally copy the differences.

	The complexity comes when there are multiple addition, deletion and compaction.
	This tool will handle them correctly and efficiently, avoiding unnecessary data transportation.
  `,
}

func runBackup(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	if *s.volumeId == -1 {
		return false
	}
	vid := needle.VolumeId(*s.volumeId)

	// find volume location, replication, ttl info
	lookup, err := operation.LookupVolumeId(func(_ context.Context) pb.ServerAddress { return pb.ServerAddress(*s.master) }, grpcDialOption, vid.String())
	if err != nil {
		fmt.Printf("Error looking up volume %d: %v\n", vid, err)
		return true
	}
	volumeServer := lookup.Locations[0].ServerAddress()

	stats, err := operation.GetVolumeSyncStatus(volumeServer, grpcDialOption, uint32(vid))
	if err != nil {
		fmt.Printf("Error get volume %d status: %v\n", vid, err)
		return true
	}
	var ttl *needle.TTL
	if *s.ttl != "" {
		ttl, err = needle.ReadTTL(*s.ttl)
		if err != nil {
			fmt.Printf("Error generate volume %d ttl %s: %v\n", vid, *s.ttl, err)
			return true
		}
	} else {
		ttl, err = needle.ReadTTL(stats.Ttl)
		if err != nil {
			fmt.Printf("Error get volume %d ttl %s: %v\n", vid, stats.Ttl, err)
			return true
		}
	}
	var replication *super_block.ReplicaPlacement
	if *s.replication != "" {
		replication, err = super_block.NewReplicaPlacementFromString(*s.replication)
		if err != nil {
			fmt.Printf("Error generate volume %d replication %s : %v\n", vid, *s.replication, err)
			return true
		}
	} else {
		replication, err = super_block.NewReplicaPlacementFromString(stats.Replication)
		if err != nil {
			fmt.Printf("Error get volume %d replication %s : %v\n", vid, stats.Replication, err)
			return true
		}
	}
	v, err := storage.NewVolume(util.ResolvePath(*s.dir), util.ResolvePath(*s.dir), *s.collection, vid, storage.NeedleMapInMemory, replication, ttl, 0, 0, 0)
	if err != nil {
		fmt.Printf("Error creating or reading from volume %d: %v\n", vid, err)
		return true
	}

	if v.SuperBlock.CompactionRevision < uint16(stats.CompactRevision) {
		if err = v.Compact2(0, 0, nil); err != nil {
			fmt.Printf("Compact Volume before synchronizing %v\n", err)
			return true
		}
		if err = v.CommitCompact(); err != nil {
			fmt.Printf("Commit Compact before synchronizing %v\n", err)
			return true
		}
		v.SuperBlock.CompactionRevision = uint16(stats.CompactRevision)
		v.DataBackend.WriteAt(v.SuperBlock.Bytes(), 0)
	}

	datSize, _, _ := v.FileStat()

	if datSize > stats.TailOffset {
		// remove the old data
		if err := v.Destroy(false); err != nil {
			fmt.Printf("Error destroying volume: %v\n", err)
		}
		// recreate an empty volume
		v, err = storage.NewVolume(util.ResolvePath(*s.dir), util.ResolvePath(*s.dir), *s.collection, vid, storage.NeedleMapInMemory, replication, ttl, 0, 0, 0)
		if err != nil {
			fmt.Printf("Error creating or reading from volume %d: %v\n", vid, err)
			return true
		}
	}
	defer v.Close()

	if err := v.IncrementalBackup(volumeServer, grpcDialOption); err != nil {
		fmt.Printf("Error synchronizing volume %d: %v\n", vid, err)
		return true
	}

	return true
}
