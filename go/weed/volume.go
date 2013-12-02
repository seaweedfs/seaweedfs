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
	cmdVolume.Run = runVolume // break init cycle
}

var cmdVolume = &Command{
	UsageLine: "volume -port=8080 -dir=/tmp -max=5 -ip=server_name -mserver=localhost:9333",
	Short:     "start a volume server",
	Long: `start a volume server to provide storage spaces

  `,
}

var (
	vport                 = cmdVolume.Flag.Int("port", 8080, "http listen port")
	volumeFolders         = cmdVolume.Flag.String("dir", os.TempDir(), "directories to store data files. dir[,dir]...")
	maxVolumeCounts       = cmdVolume.Flag.String("max", "7", "maximum numbers of volumes, count[,count]...")
	ip                    = cmdVolume.Flag.String("ip", "localhost", "ip or server name")
	publicUrl             = cmdVolume.Flag.String("publicUrl", "", "Publicly accessible <ip|server_name>:<port>")
	masterNode            = cmdVolume.Flag.String("mserver", "localhost:9333", "master server location")
	vpulse                = cmdVolume.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats, must be smaller than the master's setting")
	vReadTimeout          = cmdVolume.Flag.Int("readTimeout", 3, "connection read timeout in seconds. Increase this if uploading large files.")
	vMaxCpu               = cmdVolume.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	dataCenter            = cmdVolume.Flag.String("dataCenter", "", "current volume server's data center name")
	rack                  = cmdVolume.Flag.String("rack", "", "current volume server's rack name")
	volumeWhiteListOption = cmdVolume.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")

	volumeWhiteList []string
)

func runVolume(cmd *Command, args []string) bool {
	if *vMaxCpu < 1 {
		*vMaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*vMaxCpu)
	folders := strings.Split(*volumeFolders, ",")
	maxCountStrings := strings.Split(*maxVolumeCounts, ",")
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
		fileInfo, err := os.Stat(folder)
		if err != nil {
			glog.Fatalf("No Existing Folder:%s", folder)
		}
		if !fileInfo.IsDir() {
			glog.Fatalf("Volume Folder should not be a file:%s", folder)
		}
		perm := fileInfo.Mode().Perm()
		glog.V(0).Infoln("Volume Folder", folder)
		glog.V(0).Infoln("Permission:", perm)
	}

	if *publicUrl == "" {
		*publicUrl = *ip + ":" + strconv.Itoa(*vport)
	}
	if *volumeWhiteListOption != "" {
		volumeWhiteList = strings.Split(*volumeWhiteListOption, ",")
	}

	r := mux.NewRouter()

	weed_server.NewVolumeServer(r, VERSION, *ip, *vport, *publicUrl, folders, maxCounts,
		*masterNode, *vpulse, *dataCenter, *rack, volumeWhiteList,
	)

	glog.V(0).Infoln("Start Weed volume server", VERSION, "at http://"+*ip+":"+strconv.Itoa(*vport))
	srv := &http.Server{
		Addr:        ":" + strconv.Itoa(*vport),
		Handler:     r,
		ReadTimeout: (time.Duration(*vReadTimeout) * time.Second),
	}
	e := srv.ListenAndServe()
	if e != nil {
		glog.Fatalf("Fail to start:%s", e.Error())
	}
	return true
}
