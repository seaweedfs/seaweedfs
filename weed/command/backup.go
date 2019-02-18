package command

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/spf13/viper"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

var (
	s BackupOptions
)

type BackupOptions struct {
	master     *string
	collection *string
	dir        *string
	volumeId   *int
}

func init() {
	cmdBackup.Run = runBackup // break init cycle
	s.master = cmdBackup.Flag.String("server", "localhost:9333", "SeaweedFS master location")
	s.collection = cmdBackup.Flag.String("collection", "", "collection name")
	s.dir = cmdBackup.Flag.String("dir", ".", "directory to store volume data files")
	s.volumeId = cmdBackup.Flag.Int("volumeId", -1, "a volume id. The volume .dat and .idx files should already exist in the dir.")
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

	weed_server.LoadConfiguration("security", false)
	grpcDialOption := security.LoadClientTLS(viper.Sub("grpc"), "client")

	if *s.volumeId == -1 {
		return false
	}
	vid := storage.VolumeId(*s.volumeId)

	// find volume location, replication, ttl info
	lookup, err := operation.Lookup(*s.master, vid.String())
	if err != nil {
		fmt.Printf("Error looking up volume %d: %v\n", vid, err)
		return true
	}
	volumeServer := lookup.Locations[0].Url

	stats, err := operation.GetVolumeSyncStatus(volumeServer, grpcDialOption, uint32(vid))
	if err != nil {
		fmt.Printf("Error get volume %d status: %v\n", vid, err)
		return true
	}
	ttl, err := storage.ReadTTL(stats.Ttl)
	if err != nil {
		fmt.Printf("Error get volume %d ttl %s: %v\n", vid, stats.Ttl, err)
		return true
	}
	replication, err := storage.NewReplicaPlacementFromString(stats.Replication)
	if err != nil {
		fmt.Printf("Error get volume %d replication %s : %v\n", vid, stats.Replication, err)
		return true
	}

	v, err := storage.NewVolume(*s.dir, *s.collection, vid, storage.NeedleMapInMemory, replication, ttl, 0)
	if err != nil {
		fmt.Printf("Error creating or reading from volume %d: %v\n", vid, err)
		return true
	}

	if err := v.Synchronize(volumeServer, grpcDialOption); err != nil {
		fmt.Printf("Error synchronizing volume %d: %v\n", vid, err)
		return true
	}

	return true
}
