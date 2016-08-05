package command

import (
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
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
	serverIp                      = cmdServer.Flag.String("ip", "localhost", "ip or server name")
	serverBindIp                  = cmdServer.Flag.String("ip.bind", "0.0.0.0", "ip address to bind to")
	serverMaxCpu                  = cmdServer.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	serverTimeout                 = cmdServer.Flag.Int("idleTimeout", 10, "connection idle seconds")
	serverDataCenter              = cmdServer.Flag.String("dataCenter", "", "current volume server's data center name")
	serverRack                    = cmdServer.Flag.String("rack", "", "current volume server's rack name")
	serverReadWhiteListOption     = cmdServer.Flag.String("read.whitelist", "", "comma separated Ip addresses having read permission. No limit if empty.")
	serverWriteWhiteListOption    = cmdServer.Flag.String("write.whitelist", "", "comma separated Ip addresses having write permission. No limit if empty.")
	serverPeers                   = cmdServer.Flag.String("master.peers", "", "other master nodes in comma separated ip:masterPort list")
	serverSecureKey               = cmdServer.Flag.String("secure.secret", "", "secret to encrypt Json Web Token(JWT)")
	serverGarbageThreshold        = cmdServer.Flag.String("garbageThreshold", "0.3", "threshold to vacuum and reclaim spaces")
	masterPort                    = cmdServer.Flag.Int("master.port", 9333, "master server http listen port")
	masterMetaFolder              = cmdServer.Flag.String("master.dir", "", "data directory to store meta data, default to same as -dir specified")
	masterVolumeSizeLimitMB       = cmdServer.Flag.Uint("master.volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes.")
	masterConfFile                = cmdServer.Flag.String("master.conf", "/etc/weedfs/weedfs.conf", "xml configuration file")
	masterDefaultReplicaPlacement = cmdServer.Flag.String("master.defaultReplicaPlacement", "000", "Default replication type if not specified.")
	volumePort                    = cmdServer.Flag.Int("volume.port", 8080, "volume server http listen port")
	volumePublicPort              = cmdServer.Flag.Int("volume.port.public", 0, "volume server public port")
	volumeDataFolders             = cmdServer.Flag.String("dir", os.TempDir(), "directories to store data files. dir[,dir]...")
	volumeMaxDataVolumeCounts     = cmdServer.Flag.String("volume.max", "7", "maximum numbers of volumes, count[,count]...")
	volumePulse                   = cmdServer.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	volumeIndexType               = cmdServer.Flag.String("volume.index", "memory", "Choose [memory|leveldb|boltdb] mode for memory~performance balance.")
	volumeFixJpgOrientation       = cmdServer.Flag.Bool("volume.images.fix.orientation", true, "Adjust jpg orientation when uploading.")
	volumeReadRedirect            = cmdServer.Flag.Bool("volume.read.redirect", true, "Redirect moved or non-local volumes.")
	volumeServerPublicUrl         = cmdServer.Flag.String("volume.publicUrl", "", "publicly accessible address")
	isStartingFiler               = cmdServer.Flag.Bool("filer", false, "whether to start filer")

	serverReadWhiteList []string
	serverWriteWhiteList []string
)

func init() {
	serverOptions.cpuprofile = cmdServer.Flag.String("cpuprofile", "", "cpu profile output file")
	filerOptions.master = cmdServer.Flag.String("filer.master", "", "default to current master server")
	filerOptions.collection = cmdServer.Flag.String("filer.collection", "", "all data will be stored in this collection")
	filerOptions.port = cmdServer.Flag.Int("filer.port", 8888, "filer server http listen port")
	filerOptions.dir = cmdServer.Flag.String("filer.dir", "", "directory to store meta data, default to a 'filer' sub directory of what -mdir is specified")
	filerOptions.defaultReplicaPlacement = cmdServer.Flag.String("filer.defaultReplicaPlacement", "", "Default replication type if not specified during runtime.")
	filerOptions.redirectOnRead = cmdServer.Flag.Bool("filer.redirectOnRead", false, "whether proxy or redirect to volume server during file GET request")
	filerOptions.disableDirListing = cmdServer.Flag.Bool("filer.disableDirListing", false, "turn off directory listing")
	filerOptions.maxMB = cmdServer.Flag.Int("filer.maxMB", 0, "split files larger than the limit")
	filerOptions.cassandra_server = cmdServer.Flag.String("filer.cassandra.server", "", "host[:port] of the cassandra server")
	filerOptions.cassandra_keyspace = cmdServer.Flag.String("filer.cassandra.keyspace", "seaweed", "keyspace of the cassandra server")
	filerOptions.redis_server = cmdServer.Flag.String("filer.redis.server", "", "host:port of the redis server, e.g., 127.0.0.1:6379")
	filerOptions.redis_password = cmdServer.Flag.String("filer.redis.password", "", "redis password in clear text")
	filerOptions.redis_database = cmdServer.Flag.Int("filer.redis.database", 0, "the database on the redis server")
	filerOptions.get_ip_whitelist_option = cmdServer.Flag.String("filer.whitelist.ip.get", "", "comma separated Ip addresses having filer GET permission. No limit if empty.")
	filerOptions.get_root_whitelist_option = cmdServer.Flag.String("filer.whitelist.root.get", "", "comma separated root paths having filer GET permission. No limit if empty.")
	filerOptions.head_ip_whitelist_option = cmdServer.Flag.String("filer.whitelist.ip.head", "", "comma separated Ip addresses having filer HEAD permission. No limit if empty.")
	filerOptions.head_root_whitelist_option = cmdServer.Flag.String("filer.whitelist.root.head", "", "comma separated root paths having filer HEAD permission. No limit if empty.")
	filerOptions.delete_ip_whitelist_option = cmdServer.Flag.String("filer.whitelist.ip.delete", "", "comma separated Ip addresses having filer DELETE permission. No limit if empty.")
	filerOptions.delete_root_whitelist_option = cmdServer.Flag.String("filer.whitelist.root.delete", "", "comma separated root paths having filer DELETE permission. No limit if empty.")
	filerOptions.put_ip_whitelist_option = cmdServer.Flag.String("filer.whitelist.ip.put", "", "comma separated Ip addresses having filer PUT permission. No limit if empty.")
	filerOptions.put_root_whitelist_option = cmdServer.Flag.String("filer.whitelist.root.put", "", "comma separated root paths having filer PUT permission. No limit if empty.")
	filerOptions.post_ip_whitelist_option = cmdServer.Flag.String("filer.whitelist.ip.post", "", "comma separated Ip addresses having filer POST permission. No limit if empty.")
	filerOptions.post_root_whitelist_option = cmdServer.Flag.String("filer.whitelist.root.post", "", "comma separated root paths having filer POST permission. No limit if empty.")
	filerOptions.get_secure_key = cmdServer.Flag.String("filer.secure.secret.get", "", "secret to encrypt Json Web Token(JWT)")
	filerOptions.head_secure_key = cmdServer.Flag.String("filer.secure.secret.head", "", "secret to encrypt Json Web Token(JWT)")
	filerOptions.delete_secure_key = cmdServer.Flag.String("filer.secure.secret.delete", "", "secret to encrypt Json Web Token(JWT)")
	filerOptions.put_secure_key = cmdServer.Flag.String("filer.secure.secret.put", "", "secret to encrypt Json Web Token(JWT)")
	filerOptions.post_secure_key = cmdServer.Flag.String("filer.secure.secret.post", "", "secret to encrypt Json Web Token(JWT)")
}

func runServer(cmd *Command, args []string) bool {
	filerOptions.secretKey = serverSecureKey
	if *serverOptions.cpuprofile != "" {
		f, err := os.Create(*serverOptions.cpuprofile)
		if err != nil {
			glog.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *filerOptions.redirectOnRead {
		*isStartingFiler = true
	}

	*filerOptions.master = *serverIp + ":" + strconv.Itoa(*masterPort)

	if *filerOptions.defaultReplicaPlacement == "" {
		*filerOptions.defaultReplicaPlacement = *masterDefaultReplicaPlacement
	}

	if *volumePublicPort == 0 {
		*volumePublicPort = *volumePort
	}

	if *serverMaxCpu < 1 {
		*serverMaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*serverMaxCpu)

	folders := strings.Split(*volumeDataFolders, ",")
	maxCountStrings := strings.Split(*volumeMaxDataVolumeCounts, ",")
	var maxCounts []int
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
	if *isStartingFiler {
		if *filerOptions.dir == "" {
			*filerOptions.dir = *masterMetaFolder + "/filer"
			os.MkdirAll(*filerOptions.dir, 0700)
		}
		if err := util.TestFolderWritable(*filerOptions.dir); err != nil {
			glog.Fatalf("Check Mapping Meta Folder (-filer.dir=\"%s\") Writable: %s", *filerOptions.dir, err)
		}
		if *filerOptions.get_ip_whitelist_option != "" {
			glog.V(0).Infof("Filer GET IP whitelist: %s", *filerOptions.get_ip_whitelist_option)
			filerOptions.get_ip_whitelist = strings.Split(*filerOptions.get_ip_whitelist_option, ",")
		}
		if *filerOptions.get_root_whitelist_option != "" {
			glog.V(0).Infof("Filer GET root whitelist: %s", *filerOptions.get_root_whitelist_option)
			filerOptions.get_root_whitelist = strings.Split(*filerOptions.get_root_whitelist_option, ",")
		}
		if *filerOptions.head_ip_whitelist_option != "" {
			glog.V(0).Infof("Filer HEAD IP whitelist: %s", *filerOptions.head_ip_whitelist_option)
			filerOptions.head_ip_whitelist = strings.Split(*filerOptions.head_ip_whitelist_option, ",")
		}
		if *filerOptions.head_root_whitelist_option != "" {
			glog.V(0).Infof("Filer HEAD root whitelist: %s", *filerOptions.head_root_whitelist_option)
			filerOptions.head_root_whitelist = strings.Split(*filerOptions.head_root_whitelist_option, ",")
		}
		if *filerOptions.delete_ip_whitelist_option != "" {
			glog.V(0).Infof("Filer DELETE IP whitelist: %s", *filerOptions.delete_ip_whitelist_option)
			filerOptions.delete_ip_whitelist = strings.Split(*filerOptions.delete_ip_whitelist_option, ",")
		}
		if *filerOptions.delete_root_whitelist_option != "" {
			glog.V(0).Infof("Filer DELETE root whitelist: %s", *filerOptions.delete_root_whitelist_option)
			filerOptions.delete_root_whitelist = strings.Split(*filerOptions.delete_root_whitelist_option, ",")
		}
		if *filerOptions.put_ip_whitelist_option != "" {
			glog.V(0).Infof("Filer PUT IP whitelist: %s", *filerOptions.put_ip_whitelist_option)
			filerOptions.put_ip_whitelist = strings.Split(*filerOptions.put_ip_whitelist_option, ",")
		}
		if *filerOptions.put_root_whitelist_option != "" {
			glog.V(0).Infof("Filer PUT root whitelist: %s", *filerOptions.put_root_whitelist_option)
			filerOptions.put_root_whitelist = strings.Split(*filerOptions.put_root_whitelist_option, ",")
		}
		if *filerOptions.post_ip_whitelist_option != "" {
			glog.V(0).Infof("Filer POST IP whitelist: %s", *filerOptions.post_ip_whitelist_option)
			filerOptions.post_ip_whitelist = strings.Split(*filerOptions.post_ip_whitelist_option, ",")
		}
		if *filerOptions.post_root_whitelist_option != "" {
			glog.V(0).Infof("Filer POST root whitelist: %s", *filerOptions.post_root_whitelist_option)
			filerOptions.post_root_whitelist = strings.Split(*filerOptions.post_root_whitelist_option, ",")
		}
	}
	if err := util.TestFolderWritable(*masterMetaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir=\"%s\") Writable: %s", *masterMetaFolder, err)
	}

	if *serverReadWhiteListOption != "" {
		serverReadWhiteList = strings.Split(*serverReadWhiteListOption, ",")
	}
	if *serverWriteWhiteListOption != "" {
		serverWriteWhiteList = strings.Split(*serverWriteWhiteListOption, ",")
	}

	if *isStartingFiler {
		go func() {
			time.Sleep(1 * time.Second)
			r := http.NewServeMux()
			_, nfs_err := weed_server.NewFilerServer(r, *serverBindIp, *filerOptions.port, *filerOptions.master, *filerOptions.dir, *filerOptions.collection,
				*filerOptions.defaultReplicaPlacement,
				*filerOptions.redirectOnRead, *filerOptions.disableDirListing,
				*filerOptions.maxMB,
				*filerOptions.secretKey,
				*filerOptions.cassandra_server, *filerOptions.cassandra_keyspace,
				*filerOptions.redis_server, *filerOptions.redis_password, *filerOptions.redis_database,
				filerOptions.get_ip_whitelist, filerOptions.head_ip_whitelist, filerOptions.delete_ip_whitelist, filerOptions.put_ip_whitelist, filerOptions.post_ip_whitelist,
				filerOptions.get_root_whitelist, filerOptions.head_root_whitelist, filerOptions.delete_root_whitelist, filerOptions.put_root_whitelist, filerOptions.post_root_whitelist,
				*f.get_secure_key, *f.head_secure_key, *f.delete_secure_key, *f.put_secure_key, *f.post_secure_key,
			)
			if nfs_err != nil {
				glog.Fatalf("Filer startup error: %v", nfs_err)
			}
			glog.V(0).Infoln("Start Seaweed Filer", util.VERSION, "at port", strconv.Itoa(*filerOptions.port))
			filerListener, e := util.NewListener(
				":"+strconv.Itoa(*filerOptions.port),
				time.Duration(10)*time.Second,
			)
			if e != nil {
				glog.Fatalf("Filer listener error: %v", e)
			}
			if e := http.Serve(filerListener, r); e != nil {
				glog.Fatalf("Filer Fail to serve: %v", e)
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
			serverReadWhiteList, serverWriteWhiteList, nil, *serverSecureKey,
		)

		glog.V(0).Infoln("Start Seaweed Master", util.VERSION, "at", *serverIp+":"+strconv.Itoa(*masterPort))
		masterListener, e := util.NewListener(*serverBindIp+":"+strconv.Itoa(*masterPort), time.Duration(*serverTimeout)*time.Second)
		if e != nil {
			glog.Fatalf("Master startup error: %v", e)
		}

		go func() {
			raftWaitForMaster.Wait()
			time.Sleep(100 * time.Millisecond)
			myAddress := *serverIp + ":" + strconv.Itoa(*masterPort)
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
	if *volumePublicPort == 0 {
		*volumePublicPort = *volumePort
	}
	if *volumeServerPublicUrl == "" {
		*volumeServerPublicUrl = *serverIp + ":" + strconv.Itoa(*volumePublicPort)
	}
	isSeperatedPublicPort := *volumePublicPort != *volumePort
	volumeMux := http.NewServeMux()
	publicVolumeMux := volumeMux
	if isSeperatedPublicPort {
		publicVolumeMux = http.NewServeMux()
	}
	volumeNeedleMapKind := storage.NeedleMapInMemory
	switch *volumeIndexType {
	case "leveldb":
		volumeNeedleMapKind = storage.NeedleMapLevelDb
	case "boltdb":
		volumeNeedleMapKind = storage.NeedleMapBoltDb
	}
	volumeServer := weed_server.NewVolumeServer(volumeMux, publicVolumeMux,
		*serverIp, *volumePort, *volumeServerPublicUrl,
		folders, maxCounts,
		volumeNeedleMapKind,
		*serverIp+":"+strconv.Itoa(*masterPort), *volumePulse, *serverDataCenter, *serverRack,
		serverReadWhiteList, serverWriteWhiteList, nil, *volumeFixJpgOrientation, *volumeReadRedirect,
	)

	glog.V(0).Infoln("Start Seaweed volume server", util.VERSION, "at", *serverIp+":"+strconv.Itoa(*volumePort))
	volumeListener, eListen := util.NewListener(
		*serverBindIp+":"+strconv.Itoa(*volumePort),
		time.Duration(*serverTimeout)*time.Second,
	)
	if eListen != nil {
		glog.Fatalf("Volume server listener error: %v", eListen)
	}
	if isSeperatedPublicPort {
		publicListeningAddress := *serverIp + ":" + strconv.Itoa(*volumePublicPort)
		glog.V(0).Infoln("Start Seaweed volume server", util.VERSION, "public at", publicListeningAddress)
		publicListener, e := util.NewListener(publicListeningAddress, time.Duration(*serverTimeout)*time.Second)
		if e != nil {
			glog.Fatalf("Volume server listener error:%v", e)
		}
		go func() {
			if e := http.Serve(publicListener, publicVolumeMux); e != nil {
				glog.Fatalf("Volume server fail to serve public: %v", e)
			}
		}()
	}

	OnInterrupt(func() {
		volumeServer.Shutdown()
		pprof.StopCPUProfile()
	})

	if e := http.Serve(volumeListener, volumeMux); e != nil {
		glog.Fatalf("Volume server fail to serve:%v", e)
	}

	return true
}
