package main

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
	"code.google.com/p/weed-fs/go/weed/weed_server"
	"github.com/gorilla/mux"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func init() {
	cmdMaster.Run = runMaster // break init cycle
}

var cmdMaster = &Command{
	UsageLine: "master -port=9333",
	Short:     "start a master server",
	Long: `start a master server to provide volume=>location mapping service
  and sequence number of file ids

  `,
}

var (
	mport                   = cmdMaster.Flag.Int("port", 9333, "http listen port")
	masterIp                = cmdMaster.Flag.String("ip", "", "master listening ip address, default to listen on all network interfaces")
	mPublicIp               = cmdMaster.Flag.String("publicIp", "", "peer accessible <ip>|<server_name>")
	metaFolder              = cmdMaster.Flag.String("mdir", os.TempDir(), "data directory to store meta data")
	masterPeers             = cmdMaster.Flag.String("peers", "", "other master nodes in comma separated ip:port list")
	volumeSizeLimitMB       = cmdMaster.Flag.Uint("volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes.")
	mpulse                  = cmdMaster.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	confFile                = cmdMaster.Flag.String("conf", "/etc/weedfs/weedfs.conf", "xml configuration file")
	defaultReplicaPlacement = cmdMaster.Flag.String("defaultReplication", "000", "Default replication type if not specified.")
	mTimeout                = cmdMaster.Flag.Int("idleTimeout", 10, "connection idle seconds")
	mMaxCpu                 = cmdMaster.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	garbageThreshold        = cmdMaster.Flag.String("garbageThreshold", "0.3", "threshold to vacuum and reclaim spaces")
	masterWhiteListOption   = cmdMaster.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")

	masterWhiteList []string
)

func runMaster(cmd *Command, args []string) bool {
	if *mMaxCpu < 1 {
		*mMaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*mMaxCpu)
	if err := util.TestFolderWritable(*metaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", *metaFolder, err)
	}
	if *masterWhiteListOption != "" {
		masterWhiteList = strings.Split(*masterWhiteListOption, ",")
	}

	r := mux.NewRouter()
	ms := weed_server.NewMasterServer(r, *mport, *metaFolder,
		*volumeSizeLimitMB, *mpulse, *confFile, *defaultReplicaPlacement, *garbageThreshold, masterWhiteList,
	)

	listeningAddress := *masterIp + ":" + strconv.Itoa(*mport)

	glog.V(0).Infoln("Start Weed Master", util.VERSION, "at", listeningAddress)

	listener, e := util.NewListener(listeningAddress, time.Duration(*mTimeout)*time.Second)
	if e != nil {
		glog.Fatalf(e.Error())
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		if *mPublicIp == "" {
			if *masterIp == "" {
				*mPublicIp = "localhost"
			} else {
				*mPublicIp = *masterIp
			}
		}
		myPublicMasterAddress := *mPublicIp + ":" + strconv.Itoa(*mport)
		var peers []string
		if *masterPeers != "" {
			peers = strings.Split(*masterPeers, ",")
		}
		raftServer := weed_server.NewRaftServer(r, peers, myPublicMasterAddress, *metaFolder, ms.Topo, *mpulse)
		ms.SetRaftServer(raftServer)
	}()

	if e := http.Serve(listener, r); e != nil {
		glog.Fatalf("Fail to serve:%s", e.Error())
	}
	return true
}
