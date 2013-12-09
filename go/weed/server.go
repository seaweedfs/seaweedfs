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
	cmdServer.Run = runServer // break init cycle
}

var cmdServer = &Command{
	UsageLine: "server -port=8080 -dir=/tmp -max=5 -ip=server_name",
	Short:     "start a server, including volume server, and automatically elect a master server",
	Long: `start both a volume server to provide storage spaces 
  and a master server to provide volume=>location mapping service and sequence number of file ids
  
  This is provided as a convenient way to start both volume server and master server.
  The servers are the same as starting them separately.
  So other volume servers can use this embedded master server also.
  
  However, this may change very soon.
  The target is to start both volume server and embedded master server on all instances,
  and use a leader election process to auto choose a master server.

  `,
}

var (
	serverIp                  = cmdServer.Flag.String("ip", "localhost", "ip or server name")
	serverMaxCpu              = cmdServer.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	serverReadTimeout         = cmdServer.Flag.Int("readTimeout", 3, "connection read timeout in seconds. Increase this if uploading large files.")
	serverDataCenter          = cmdServer.Flag.String("dataCenter", "", "current volume server's data center name")
	serverRack                = cmdServer.Flag.String("rack", "", "current volume server's rack name")
	serverWhiteListOption     = cmdServer.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	serverPeers               = cmdServer.Flag.String("peers", "", "comma separated ip:masterPort list")
	masterPort                = cmdServer.Flag.Int("masterPort", 9333, "master server http listen port")
	masterMetaFolder          = cmdServer.Flag.String("mdir", os.TempDir(), "data directory to store meta data")
	masterVolumeSizeLimitMB   = cmdServer.Flag.Uint("volumeSizeLimitMB", 32*1024, "Default Volume Size in MegaBytes")
	masterConfFile            = cmdServer.Flag.String("conf", "/etc/weedfs/weedfs.conf", "xml configuration file")
	masterDefaultRepType      = cmdServer.Flag.String("defaultReplicationType", "000", "Default replication type if not specified.")
	volumePort                = cmdServer.Flag.Int("port", 8080, "volume server http listen port")
	volumePublicUrl           = cmdServer.Flag.String("publicUrl", "", "Publicly accessible <ip|server_name>:<port>")
	volumeDataFolders         = cmdServer.Flag.String("dir", os.TempDir(), "directories to store data files. dir[,dir]...")
	volumeMaxDataVolumeCounts = cmdServer.Flag.String("max", "7", "maximum numbers of volumes, count[,count]...")
	volumePulse               = cmdServer.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats, must be smaller than the master's setting")

	serverWhiteList []string
)

func runServer(cmd *Command, args []string) bool {
	if *serverMaxCpu < 1 {
		*serverMaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*serverMaxCpu)

	if err := util.TestFolderWritable(*masterMetaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", *masterMetaFolder, err)
	}

	folders := strings.Split(*volumeDataFolders, ",")
	maxCountStrings := strings.Split(*volumeMaxDataVolumeCounts, ",")
	maxCounts := make([]int, 0)
	for _, maxString := range maxCountStrings {
		if max, e := strconv.Atoi(maxString); e == nil {
			maxCounts = append(maxCounts, max)
		} else {
			glog.Fatalf("The max specified in -max not a valid number %s", max)
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

	if *volumePublicUrl == "" {
		*volumePublicUrl = *serverIp + ":" + strconv.Itoa(*volumePort)
	}
	if *serverWhiteListOption != "" {
		serverWhiteList = strings.Split(*serverWhiteListOption, ",")
	}

	go func() {
		r := mux.NewRouter()
		weed_server.NewMasterServer(r, VERSION, *masterPort, *masterMetaFolder,
			*masterVolumeSizeLimitMB, *volumePulse, *masterConfFile, *masterDefaultRepType, *garbageThreshold, serverWhiteList,
		)

		glog.V(0).Infoln("Start Weed Master", VERSION, "at port", *serverIp+":"+strconv.Itoa(*masterPort))
		masterServer := &http.Server{
			Addr:        *serverIp + ":" + strconv.Itoa(*masterPort),
			Handler:     r,
			ReadTimeout: time.Duration(*serverReadTimeout) * time.Second,
		}

		go func() {
			time.Sleep(100 * time.Millisecond)
			var peers []string
			if *serverPeers != "" {
				peers = strings.Split(*serverPeers, ",")
			}
			weed_server.NewRaftServer(r, VERSION, peers, *serverIp+":"+strconv.Itoa(*masterPort), *masterMetaFolder)
		}()

		e := masterServer.ListenAndServe()
		if e != nil {
			glog.Fatalf("Fail to start master:%s", e)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	r := http.NewServeMux()
	weed_server.NewVolumeServer(r, VERSION, *serverIp, *volumePort, *volumePublicUrl, folders, maxCounts,
		*serverIp+":"+strconv.Itoa(*masterPort), *volumePulse, *serverDataCenter, *serverRack, serverWhiteList,
	)

	glog.V(0).Infoln("Start Weed volume server", VERSION, "at http://"+*serverIp+":"+strconv.Itoa(*volumePort))
	volumeServer := &http.Server{
		Addr:        *serverIp + ":" + strconv.Itoa(*volumePort),
		Handler:     r,
		ReadTimeout: (time.Duration(*serverReadTimeout) * time.Second),
	}
	e := volumeServer.ListenAndServe()
	if e != nil {
		glog.Fatalf("Fail to start volume:%s", e.Error())
	}

	return true
}
