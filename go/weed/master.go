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
	masterIp                = cmdMaster.Flag.String("ip", "localhost", "master ip address")
	metaFolder              = cmdMaster.Flag.String("mdir", os.TempDir(), "data directory to store meta data")
	masterPeers             = cmdMaster.Flag.String("peers", "", "other master nodes in comma separated ip:port list")
	volumeSizeLimitMB       = cmdMaster.Flag.Uint("volumeSizeLimitMB", 32*1000, "Default Volume Size in MegaBytes")
	mpulse                  = cmdMaster.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	confFile                = cmdMaster.Flag.String("conf", "/etc/weedfs/weedfs.conf", "xml configuration file")
	defaultReplicaPlacement = cmdMaster.Flag.String("defaultReplication", "000", "Default replication type if not specified.")
	mReadTimeout            = cmdMaster.Flag.Int("readTimeout", 3, "connection read timeout in seconds")
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
	ms := weed_server.NewMasterServer(r, VERSION, *mport, *metaFolder,
		*volumeSizeLimitMB, *mpulse, *confFile, *defaultReplicaPlacement, *garbageThreshold, masterWhiteList,
	)

	glog.V(0).Infoln("Start Weed Master", VERSION, "at port", *masterIp+":"+strconv.Itoa(*mport))

	srv := &http.Server{
		Addr:        *masterIp + ":" + strconv.Itoa(*mport),
		Handler:     r,
		ReadTimeout: time.Duration(*mReadTimeout) * time.Second,
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		var peers []string
		if *masterPeers != "" {
			peers = strings.Split(*masterPeers, ",")
		}
		raftServer := weed_server.NewRaftServer(r, VERSION, peers, *masterIp+":"+strconv.Itoa(*mport), *metaFolder)
		ms.SetRaftServer(raftServer)
	}()

	e := srv.ListenAndServe()
	if e != nil {
		glog.Fatalf("Fail to start:%s", e)
	}
	return true
}
