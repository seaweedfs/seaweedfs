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
	server      *string // deprecated, for backward compatibility
	collection  *string
	dir         *string
	volumeId    *int
	ttl         *string
	replication *string
}

func init() {
	cmdBackup.Run = runBackup // break init cycle
	s.master = cmdBackup.Flag.String("master", "localhost:9333", "SeaweedFS master location")
	s.server = cmdBackup.Flag.String("server", "", "SeaweedFS master location (deprecated, use -master instead)")
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
	UsageLine: "backup -dir=. -volumeId=234 -master=localhost:9333",
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

	// Backward compatibility: if -server is provided, use it
	masterServer := *s.master
	if *s.server != "" {
		masterServer = *s.server
	}

	if *s.volumeId == -1 {
		return false
	}
	vid := needle.VolumeId(*s.volumeId)

	// find volume location, replication, ttl info
	lookup, err := operation.LookupVolumeId(func(_ context.Context) pb.ServerAddress { return pb.ServerAddress(masterServer) }, grpcDialOption, vid.String())
	if err != nil {
		fmt.Printf("Error looking up volume %d: %v\n", vid, err)
		return true
	}
	if len(lookup.Locations) == 0 {
		fmt.Printf("Error: volume %d has no locations available\n", vid)
		return true
	}

	// Try each available location until one succeeds
	var lastErr error
	for i, location := range lookup.Locations {
		volumeServer := location.ServerAddress()
		fmt.Printf("Attempting to backup volume %d from location %d/%d: %s\n", vid, i+1, len(lookup.Locations), volumeServer)

		stats, err := operation.GetVolumeSyncStatus(volumeServer, grpcDialOption, uint32(vid))
		if err != nil {
			fmt.Printf("Error getting volume %d status from %s: %v\n", vid, volumeServer, err)
			lastErr = err
			continue
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
				fmt.Printf("Error parsing volume %d ttl %s from %s: %v\n", vid, stats.Ttl, volumeServer, err)
				lastErr = err
				continue
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
				fmt.Printf("Error parsing volume %d replication %s from %s: %v\n", vid, stats.Replication, volumeServer, err)
				lastErr = err
				continue
			}
		}

		ver := needle.Version(stats.Version)

		v, err := storage.NewVolume(util.ResolvePath(*s.dir), util.ResolvePath(*s.dir), *s.collection, vid, storage.NeedleMapInMemory, replication, ttl, 0, ver, 0, 0)
		if err != nil {
			fmt.Printf("Error creating or reading from volume %d from %s: %v\n", vid, volumeServer, err)
			lastErr = err
			continue
		}

		if v.SuperBlock.CompactionRevision < uint16(stats.CompactRevision) {
			if err = v.Compact2(0, 0, nil); err != nil {
				fmt.Printf("Compact Volume before synchronizing from %s: %v\n", volumeServer, err)
				v.Close()
				lastErr = err
				continue
			}
			if err = v.CommitCompact(); err != nil {
				fmt.Printf("Commit Compact before synchronizing from %s: %v\n", volumeServer, err)
				v.Close()
				lastErr = err
				continue
			}
			v.SuperBlock.CompactionRevision = uint16(stats.CompactRevision)
			if _, err = v.DataBackend.WriteAt(v.SuperBlock.Bytes(), 0); err != nil {
				fmt.Printf("Error writing superblock from %s: %v\n", volumeServer, err)
				v.Close()
				lastErr = err
				continue
			}
		}

		datSize, _, _ := v.FileStat()

		if datSize > stats.TailOffset {
			// remove the old data
			if err := v.Destroy(false); err != nil {
				fmt.Printf("Error destroying volume %d on %s: %v\n", vid, volumeServer, err)
				v.Close()
				lastErr = err
				continue
			}
			v.Close() // Close the old volume handle before creating a new one
			// recreate an empty volume
			v, err = storage.NewVolume(util.ResolvePath(*s.dir), util.ResolvePath(*s.dir), *s.collection, vid, storage.NeedleMapInMemory, replication, ttl, 0, ver, 0, 0)
			if err != nil {
				fmt.Printf("Error recreating volume %d from %s: %v\n", vid, volumeServer, err)
				lastErr = err
				continue
			}
		}

		// Try the incremental backup
		if err := v.IncrementalBackup(volumeServer, grpcDialOption); err != nil {
			fmt.Printf("Error synchronizing volume %d from %s: %v\n", vid, volumeServer, err)
			v.Close()
			lastErr = err
			continue
		}

		// Success!
		v.Close()
		fmt.Printf("Successfully backed up volume %d from %s\n", vid, volumeServer)
		return true
	}

	// All locations failed
	fmt.Printf("Failed to backup volume %d after trying all %d locations. Last error: %v\n", vid, len(lookup.Locations), lastErr)

	return true
}
