package main

import (
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/util"
	"os"
	"path"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/storage"
)

var (
	fixVolumePath       = flag.String("dir", "/tmp", "data directory to store files")
	fixVolumeCollection = flag.String("collection", "", "the volume collection name")
	fixVolumeId         = flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir. The volume index file should not exist.")
	targetReplica       = flag.String("replication", "", "If just empty, only print out current replication setting.")
	targetTTL           = flag.String("ttl", "", "If just empty, only print out current ttl setting.")
)

/*
This is to change replication factor in .dat file header. Need to shut down the volume servers
that has those volumes.

1. fix the .dat file in place
	// just see the replication setting
	go run change_replication.go -volumeId=9 -dir=/Users/chrislu/Downloads
		Current Volume Replication: 000
	// fix the replication setting
	go run change_replication.go -volumeId=9 -dir=/Users/chrislu/Downloads -replication 001
		Current Volume Replication: 000
		Changing to: 001
		Done.

2. copy the fixed .dat and related .idx files to some remote server
3. restart volume servers or start new volume servers.
*/
func main() {
	flag.Parse()
	fileName := strconv.Itoa(*fixVolumeId)
	if *fixVolumeCollection != "" {
		fileName = *fixVolumeCollection + "_" + fileName
	}
	datFile, err := os.OpenFile(path.Join(*fixVolumePath, fileName+".dat"), os.O_RDWR, 0644)
	util.LogFatalIfError(err, "Open Volume Data File [ERROR]: %v", err)
	defer datFile.Close()

	superBlock, err := storage.ReadSuperBlock(datFile)
	util.LogFatalIfError(err, "cannot parse existing super block: %v", err)

	fmt.Printf("Current Volume Replication: %s\n", superBlock.ReplicaPlacement)
	fmt.Printf("Current Volume TTL: %s\n", superBlock.Ttl.String())

	hasChange := false

	if *targetReplica != "" {
		replica, err := storage.NewReplicaPlacementFromString(*targetReplica)
		util.LogFatalIfError(err, "cannot parse target replica %s: %v", *targetReplica, err)

		fmt.Printf("Changing replication to: %s\n", replica)

		superBlock.ReplicaPlacement = replica
		hasChange = true
	}

	if *targetTTL != "" {
		ttl, err := storage.ReadTTL(*targetTTL)
		util.LogFatalIfError(err, "cannot parse target ttl %s: %v", *targetTTL, err)

		fmt.Printf("Changing ttl to: %s\n", ttl)

		superBlock.Ttl = ttl
		hasChange = true
	}

	if hasChange {
		header := superBlock.Bytes()

		n, e := datFile.WriteAt(header, 0)
		util.LogFatalIf(n == 0 || e != nil, "cannot write super block: %v", e)

		fmt.Println("Change Applied.")
	}

}
