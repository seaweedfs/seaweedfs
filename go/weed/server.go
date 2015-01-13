package main

import (
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chrislusf/weed-fs/go/glog"
	"github.com/chrislusf/weed-fs/go/util"
	"github.com/chrislusf/weed-fs/go/weed/weed_server"
	"github.com/gorilla/mux"
)

type ServerOptions struct {
	cpuprofile *string
}

var (
	serverOptions ServerOptions
	filerOptions  FilerOptions
)

func init() {
	cmdServer.Run = runServer // break init cycle
}

var cmdServer = &Command{
	UsageLine: "server -port=8080 -dir=/tmp -volume.max=5 -ip=server_name",
	Short:     "start a server, including volume server, and automatically elect a master server",
	Long: `start both a volume server to provide storage spaces
  and a master server to provide volume=>location mapping service and sequence number of file ids

  This is provided as a convenient way to start both volume server and master server.
  The servers are exactly the same as starting them separately.

  So other volume servers can use this embedded master server also.

  Optionally, one filer server can be started. Logically, filer servers should not be in a cluster.
  They run with meta data on disk, not shared. So each filer server is different.

  `,
}

var (
	serverIp                      = cmdServer.Flag.String("ip", "", "ip or server name")
	serverPublicIp                = cmdServer.Flag.String("publicIp", "", "ip or server name")
	serverBindIp                  = cmdServer.Flag.String("ip.bind", "0.0.0.0", "ip address to bind to")
	serverMaxCpu                  = cmdServer.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	serverTimeout                 = cmdServer.Flag.Int("idleTimeout", 10, "connection idle seconds")
	serverDataCenter              = cmdServer.Flag.String("dataCenter", "", "current volume server's data center name")
	serverRack                    = cmdServer.Flag.String("rack", "", "current volume server's rack name")
	serverWhiteListOption         = cmdServer.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	serverPeers                   = cmdServer.Flag.String("master.peers", "", "other master nodes in comma separated ip:masterPort list")
	serverSecureKey               = cmdServer.Flag.String("secure.key", "", "secret key to ensure authenticated access")
	serverGarbageThreshold        = cmdServer.Flag.String("garbageThreshold", "0.3", "threshold to vacuum and reclaim spaces")
	masterPort                    = cmdServer.Flag.Int("master.port", 9333, "master server http listen port")
	masterMetaFolder              = cmdServer.Flag.String("master.dir", "", "data directory to store meta data, default to same as -dir specified")
	masterVolumeSizeLimitMB       = cmdServer.Flag.Uint("master.volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes.")
	masterConfFile                = cmdServer.Flag.String("master.conf", "/etc/weedfs/weedfs.conf", "xml configuration file")
	masterDefaultReplicaPlacement = cmdServer.Flag.String("master.defaultReplicaPlacement", "000", "Default replication type if not specified.")
	volumePort                    = cmdServer.Flag.Int("volume.port", 8080, "volume server http listen port")
	volumeDataFolders             = cmdServer.Flag.String("dir", os.TempDir(), "directories to store data files. dir[,dir]...")
	volumeMaxDataVolumeCounts     = cmdServer.Flag.String("volume.max", "7", "maximum numbers of volumes, count[,count]...")
	volumePulse                   = cmdServer.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	volumeFixJpgOrientation       = cmdServer.Flag.Bool("volume.images.fix.orientation", true, "Adjust jpg orientation when uploading.")
	isStartingFiler               = cmdServer.Flag.Bool("filer", false, "whether to start filer")

	serverWhiteList []string
)

func init() {
	serverOptions.cpuprofile = cmdServer.Flag.String("cpuprofile", "", "cpu profile output file")
	filerOptions.master = cmdServer.Flag.String("filer.master", "", "default to current master server")
	filerOptions.collection = cmdServer.Flag.String("filer.collection", "", "all data will be stored in this collection")
	filerOptions.port = cmdServer.Flag.Int("filer.port", 8888, "filer server http listen port")
	filerOptions.dir = cmdServer.Flag.String("filer.dir", "", "directory to store meta data, default to a 'filer' sub directory of what -mdir is specified")
	filerOptions.defaultReplicaPlacement = cmdServer.Flag.String("filer.defaultReplicaPlacement", "", "Default replication type if not specified during runtime.")
	filerOptions.redirectOnRead = cmdServer.Flag.Bool("filer.redirectOnRead", false, "whether proxy or redirect to volume server during file GET request")
	filerOptions.cassandra_server = cmdServer.Flag.String("filer.cassandra.server", "", "host[:port] of the cassandra server")
	filerOptions.cassandra_keyspace = cmdServer.Flag.String("filer.cassandra.keyspace", "seaweed", "keyspace of the cassandra server")
	filerOptions.redis_server = cmdServer.Flag.String("filer.redis.server", "", "host:port of the redis server, e.g., 127.0.0.1:6379")
	filerOptions.redis_database = cmdServer.Flag.Int("filer.redis.database", 0, "the database on the redis server")

}

func runServer(cmd *Command, args []string) bool {
	if *serverOptions.cpuprofile != "" {
		f, err := os.Create(*serverOptions.cpuprofile)
		if err != nil {
			glog.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *serverPublicIp == "" {
		if *serverIp == "" {
			*serverPublicIp = "localhost"
		} else {
			*serverPublicIp = *serverIp
		}
	}

	if *filerOptions.redirectOnRead {
		*isStartingFiler = true
	}

	*filerOptions.master = *serverPublicIp + ":" + strconv.Itoa(*masterPort)

	if *filerOptions.defaultReplicaPlacement == "" {
		*filerOptions.defaultReplicaPlacement = *masterDefaultReplicaPlacement
	}

	if *serverMaxCpu < 1 {
		*serverMaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*serverMaxCpu)

	folders := strings.Split(*volumeDataFolders, ",")
	maxCountStrings := strings.Split(*volumeMaxDataVolumeCounts, ",")
	maxCounts := make([]int, 0)
	for _, maxString := range maxCountStrings {
		if max, e := strconv.Atoi(maxString); e == nil {
			maxCounts = append(maxCounts, max)
		} else {
			glog.Fatalf("The max specified in -max not a valid number %s", maxString)
		}
	}
	if len(folders) != len(maxCounts) {
		glog.Fatalf("%d directories by -dir, but only %d max is set by -max", len(folders), len(maxCounts))
	}
	for _, folder := range folders {
		if err := util.TestFolderWritable(folder); err != nil {
			glog.Fatalf("Check Data Folder(-dir) Writable %s : %s", folder, err)
		}
	}

	if *masterMetaFolder == "" {
		*masterMetaFolder = folders[0]
	}
	if *filerOptions.dir == "" {
		*filerOptions.dir = *masterMetaFolder + "/filer"
		os.MkdirAll(*filerOptions.dir, 0700)
	}
	if err := util.TestFolderWritable(*masterMetaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir=\"%s\") Writable: %s", *masterMetaFolder, err)
	}
	if err := util.TestFolderWritable(*filerOptions.dir); err != nil {
		glog.Fatalf("Check Mapping Meta Folder (-filer.dir=\"%s\") Writable: %s", *filerOptions.dir, err)
	}

	if *serverWhiteListOption != "" {
		serverWhiteList = strings.Split(*serverWhiteListOption, ",")
	}

	if *isStartingFiler {
		go func() {
			r := http.NewServeMux()
			_, nfs_err := weed_server.NewFilerServer(r, *filerOptions.port, *filerOptions.master, *filerOptions.dir, *filerOptions.collection,
				*filerOptions.defaultReplicaPlacement, *filerOptions.redirectOnRead,
				"", "",
				"", 0,
			)
			if nfs_err != nil {
				glog.Fatalf(nfs_err.Error())
			}
			glog.V(0).Infoln("Start Seaweed Filer", util.VERSION, "at port", strconv.Itoa(*filerOptions.port))
			filerListener, e := util.NewListener(
				":"+strconv.Itoa(*filerOptions.port),
				time.Duration(10)*time.Second,
			)
			if e != nil {
				glog.Fatalf(e.Error())
			}
			if e := http.Serve(filerListener, r); e != nil {
				glog.Fatalf("Filer Fail to serve:%s", e.Error())
			}
		}()
	}

	var raftWaitForMaster sync.WaitGroup
	var volumeWait sync.WaitGroup

	raftWaitForMaster.Add(1)
	volumeWait.Add(1)

	go func() {
		r := mux.NewRouter()
		ms := weed_server.NewMasterServer(r, *masterPort, *masterMetaFolder,
			*masterVolumeSizeLimitMB, *volumePulse, *masterConfFile, *masterDefaultReplicaPlacement, *serverGarbageThreshold,
			serverWhiteList, *serverSecureKey,
		)

		glog.V(0).Infoln("Start Seaweed Master", util.VERSION, "at", *serverIp+":"+strconv.Itoa(*masterPort))
		masterListener, e := util.NewListener(*serverBindIp+":"+strconv.Itoa(*masterPort), time.Duration(*serverTimeout)*time.Second)
		if e != nil {
			glog.Fatalf(e.Error())
		}

		go func() {
			raftWaitForMaster.Wait()
			time.Sleep(100 * time.Millisecond)
			myAddress := *serverPublicIp + ":" + strconv.Itoa(*masterPort)
			var peers []string
			if *serverPeers != "" {
				peers = strings.Split(*serverPeers, ",")
			}
			raftServer := weed_server.NewRaftServer(r, peers, myAddress, *masterMetaFolder, ms.Topo, *volumePulse)
			ms.SetRaftServer(raftServer)
			volumeWait.Done()
		}()

		raftWaitForMaster.Done()
		if e := http.Serve(masterListener, r); e != nil {
			glog.Fatalf("Master Fail to serve:%s", e.Error())
		}
	}()

	volumeWait.Wait()
	time.Sleep(100 * time.Millisecond)
	r := http.NewServeMux()
	volumeServer := weed_server.NewVolumeServer(r, r, *serverIp, *volumePort, *serverPublicIp, folders, maxCounts,
		*serverIp+":"+strconv.Itoa(*masterPort), *volumePulse, *serverDataCenter, *serverRack,
		serverWhiteList, *volumeFixJpgOrientation,
	)

	glog.V(0).Infoln("Start Seaweed volume server", util.VERSION, "at", *serverIp+":"+strconv.Itoa(*volumePort))
	volumeListener, e := util.NewListener(
		*serverBindIp+":"+strconv.Itoa(*volumePort),
		time.Duration(*serverTimeout)*time.Second,
	)
	if e != nil {
		glog.Fatalf(e.Error())
	}

	OnInterrupt(func() {
		volumeServer.Shutdown()
		pprof.StopCPUProfile()
	})

	if e := http.Serve(volumeListener, r); e != nil {
		glog.Fatalf("Fail to serve:%s", e.Error())
	}

	return true
}
