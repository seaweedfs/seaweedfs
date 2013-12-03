package main

import (
	"code.google.com/p/weed-fs/go/glog"
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
	mport                 = cmdMaster.Flag.Int("port", 9333, "http listen port")
	mip                   = cmdMaster.Flag.String("ip", "localhost", "http listen port")
	metaFolder            = cmdMaster.Flag.String("mdir", os.TempDir(), "data directory to store mappings")
	volumeSizeLimitMB     = cmdMaster.Flag.Uint("volumeSizeLimitMB", 32*1024, "Default Volume Size in MegaBytes")
	mpulse                = cmdMaster.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	confFile              = cmdMaster.Flag.String("conf", "/etc/weedfs/weedfs.conf", "xml configuration file")
	defaultRepType        = cmdMaster.Flag.String("defaultReplicationType", "000", "Default replication type if not specified.")
	mReadTimeout          = cmdMaster.Flag.Int("readTimeout", 3, "connection read timeout in seconds")
	mMaxCpu               = cmdMaster.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	garbageThreshold      = cmdMaster.Flag.String("garbageThreshold", "0.3", "threshold to vacuum and reclaim spaces")
	masterWhiteListOption = cmdMaster.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	//etcdCluster           = cmdMaster.Flag.String("etcd", "", "comma separated etcd urls, e.g., http://localhost:4001, See github.com/coreos/go-etcd/etcd")

	masterWhiteList []string
)

func runMaster(cmd *Command, args []string) bool {
	if *mMaxCpu < 1 {
		*mMaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*mMaxCpu)
	if *masterWhiteListOption != "" {
		masterWhiteList = strings.Split(*masterWhiteListOption, ",")
	}

	r := mux.NewRouter()
	weed_server.NewMasterServer(r, VERSION, *mport, *metaFolder,
		*volumeSizeLimitMB, *mpulse, *confFile, *defaultRepType, *garbageThreshold, masterWhiteList,
	)

	glog.V(0).Infoln("Start Weed Master", VERSION, "at port", *mip+":"+strconv.Itoa(*mport))
	srv := &http.Server{
		Addr:        *mip+":" + strconv.Itoa(*mport),
		Handler:     r,
		ReadTimeout: time.Duration(*mReadTimeout) * time.Second,
	}
	e := srv.ListenAndServe()
	if e != nil {
		glog.Fatalf("Fail to start:%s", e)
	}
	return true
}
