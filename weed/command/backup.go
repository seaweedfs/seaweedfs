package command

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

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

// parseTTL parses the TTL from user input or volume stats.
// Returns (ttl, error, isFatal) where isFatal=true for invalid user input.
func parseTTL(userTTL string, statsTTL string) (*needle.TTL, error, bool) {
	if userTTL != "" {
		ttl, err := needle.ReadTTL(userTTL)
		if err != nil {
			// User-provided TTL is invalid - this is fatal
			return nil, fmt.Errorf("invalid user-provided ttl %s: %w", userTTL, err), true
		}
		return ttl, nil, false
	}

	ttl, err := needle.ReadTTL(statsTTL)
	if err != nil {
		return nil, fmt.Errorf("parsing ttl %s from stats: %w", statsTTL, err), false
	}
	return ttl, nil, false
}

// parseReplication parses the replication from user input or volume stats.
// Returns (replication, error, isFatal) where isFatal=true for invalid user input.
func parseReplication(userReplication string, statsReplication string) (*super_block.ReplicaPlacement, error, bool) {
	if userReplication != "" {
		replication, err := super_block.NewReplicaPlacementFromString(userReplication)
		if err != nil {
			// User-provided replication is invalid - this is fatal
			return nil, fmt.Errorf("invalid user-provided replication %s: %w", userReplication, err), true
		}
		return replication, nil, false
	}

	replication, err := super_block.NewReplicaPlacementFromString(statsReplication)
	if err != nil {
		return nil, fmt.Errorf("parsing replication %s from stats: %w", statsReplication, err), false
	}
	return replication, nil, false
}

// backupFromLocation attempts to backup a volume from a specific volume server location.
// Returns (error, isFatal) where isFatal=true means the error is due to invalid user input
// and should not be retried with other locations.
func backupFromLocation(volumeServer pb.ServerAddress, grpcDialOption grpc.DialOption, vid needle.VolumeId) (error, bool) {
	stats, err := operation.GetVolumeSyncStatus(volumeServer, grpcDialOption, uint32(vid))
	if err != nil {
		return fmt.Errorf("getting volume status: %w", err), false
	}

	// Parse TTL
	ttl, err, isFatal := parseTTL(*s.ttl, stats.Ttl)
	if err != nil {
		return err, isFatal
	}

	// Parse replication
	replication, err, isFatal := parseReplication(*s.replication, stats.Replication)
	if err != nil {
		return err, isFatal
	}

	ver := needle.Version(stats.Version)

	// Create or load the volume
	v, err := storage.NewVolume(util.ResolvePath(*s.dir), util.ResolvePath(*s.dir), *s.collection, vid, storage.NeedleMapInMemory, replication, ttl, 0, ver, 0, 0)
	if err != nil {
		return fmt.Errorf("creating or reading volume: %w", err), false
	}
	defer v.Close()

	// Handle compaction if needed
	if v.SuperBlock.CompactionRevision < uint16(stats.CompactRevision) {
		if err = v.Compact2(0, 0, nil); err != nil {
			return fmt.Errorf("compacting volume: %w", err), false
		}
		if err = v.CommitCompact(); err != nil {
			return fmt.Errorf("committing compaction: %w", err), false
		}
		v.SuperBlock.CompactionRevision = uint16(stats.CompactRevision)
		if _, err = v.DataBackend.WriteAt(v.SuperBlock.Bytes(), 0); err != nil {
			return fmt.Errorf("writing superblock: %w", err), false
		}
	}

	datSize, _, _ := v.FileStat()

	// If local volume is larger than remote, recreate it
	if datSize > stats.TailOffset {
		if err := v.Destroy(false); err != nil {
			return fmt.Errorf("destroying volume: %w", err), false
		}
		v.Close()
		// recreate an empty volume
		v, err = storage.NewVolume(util.ResolvePath(*s.dir), util.ResolvePath(*s.dir), *s.collection, vid, storage.NeedleMapInMemory, replication, ttl, 0, ver, 0, 0)
		if err != nil {
			return fmt.Errorf("recreating volume: %w", err), false
		}
		defer v.Close()
	}

	// Perform the incremental backup
	if err := v.IncrementalBackup(volumeServer, grpcDialOption); err != nil {
		return fmt.Errorf("incremental backup: %w", err), false
	}

	return nil, false
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

		err, isFatal := backupFromLocation(volumeServer, grpcDialOption, vid)
		if err != nil {
			fmt.Printf("Error backing up volume %d from %s: %v\n", vid, volumeServer, err)
			lastErr = err
			// Check if this is a fatal user-input error
			if isFatal {
				return true
			}
			continue
		}

		// Success!
		fmt.Printf("Successfully backed up volume %d from %s\n", vid, volumeServer)
		return true
	}

	// All locations failed
	fmt.Printf("Failed to backup volume %d after trying all %d locations. Last error: %v\n", vid, len(lookup.Locations), lastErr)

	return true
}
