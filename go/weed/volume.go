package main

import (
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/weed-fs/go/glog"
	"github.com/chrislusf/weed-fs/go/util"
	"github.com/chrislusf/weed-fs/go/weed/weed_server"
)

var (
	v VolumeServerOptions
)

type VolumeServerOptions struct {
	port                  *int
	adminPort             *int
	folders               []string
	folderMaxLimits       []int
	ip                    *string
	publicIp              *string
	bindIp                *string
	master                *string
	pulseSeconds          *int
	idleConnectionTimeout *int
	maxCpu                *int
	dataCenter            *string
	rack                  *string
	whiteList             []string
	fixJpgOrientation     *bool
}

func init() {
	cmdVolume.Run = runVolume // break init cycle
	v.port = cmdVolume.Flag.Int("port", 8080, "http listen port")
	v.adminPort = cmdVolume.Flag.Int("port.admin", 8443, "https admin port, active when SSL certs are specified. Not ready yet.")
	v.ip = cmdVolume.Flag.String("ip", "", "ip or server name")
	v.publicIp = cmdVolume.Flag.String("publicIp", "", "Publicly accessible <ip|server_name>")
	v.bindIp = cmdVolume.Flag.String("ip.bind", "0.0.0.0", "ip address to bind to")
	v.master = cmdVolume.Flag.String("mserver", "localhost:9333", "master server location")
	v.pulseSeconds = cmdVolume.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats, must be smaller than or equal to the master's setting")
	v.idleConnectionTimeout = cmdVolume.Flag.Int("idleTimeout", 10, "connection idle seconds")
	v.maxCpu = cmdVolume.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	v.dataCenter = cmdVolume.Flag.String("dataCenter", "", "current volume server's data center name")
	v.rack = cmdVolume.Flag.String("rack", "", "current volume server's rack name")
	v.fixJpgOrientation = cmdVolume.Flag.Bool("images.fix.orientation", true, "Adjust jpg orientation when uploading.")
}

var cmdVolume = &Command{
	UsageLine: "volume -port=8080 -dir=/tmp -max=5 -ip=server_name -mserver=localhost:9333",
	Short:     "start a volume server",
	Long: `start a volume server to provide storage spaces

  `,
}

var (
	volumeFolders         = cmdVolume.Flag.String("dir", os.TempDir(), "directories to store data files. dir[,dir]...")
	maxVolumeCounts       = cmdVolume.Flag.String("max", "7", "maximum numbers of volumes, count[,count]...")
	volumeWhiteListOption = cmdVolume.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
)

func runVolume(cmd *Command, args []string) bool {
	if *v.maxCpu < 1 {
		*v.maxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*v.maxCpu)

	v.folders = strings.Split(*volumeFolders, ",")
	maxCountStrings := strings.Split(*maxVolumeCounts, ",")
	for _, maxString := range maxCountStrings {
		if max, e := strconv.Atoi(maxString); e == nil {
			v.folderMaxLimits = append(v.folderMaxLimits, max)
		} else {
			glog.Fatalf("The max specified in -max not a valid number %s", maxString)
		}
	}
	if len(v.folders) != len(v.folderMaxLimits) {
		glog.Fatalf("%d directories by -dir, but only %d max is set by -max", len(v.folders), len(v.folderMaxLimits))
	}
	for _, folder := range v.folders {
		if err := util.TestFolderWritable(folder); err != nil {
			glog.Fatalf("Check Data Folder(-dir) Writable %s : %s", folder, err)
		}
	}
	if *volumeWhiteListOption != "" {
		v.whiteList = strings.Split(*volumeWhiteListOption, ",")
	}

	if *v.publicIp == "" {
		if *v.ip == "" {
			*v.ip = "127.0.0.1"
			*v.publicIp = "localhost"
		} else {
			*v.publicIp = *v.ip
		}
	}

	r := http.NewServeMux()

	volumeServer := weed_server.NewVolumeServer(r, *v.ip, *v.port, *v.publicIp, v.folders, v.folderMaxLimits,
		*v.master, *v.pulseSeconds, *v.dataCenter, *v.rack,
		v.whiteList,
		*v.fixJpgOrientation,
	)

	listeningAddress := *v.ip + ":" + strconv.Itoa(*v.port)

	glog.V(0).Infoln("Start Seaweed volume server", util.VERSION, "at", listeningAddress)

	listener, e := util.NewListener(listeningAddress, time.Duration(*v.idleConnectionTimeout)*time.Second)
	if e != nil {
		glog.Fatalf(e.Error())
	}

	OnInterrupt(func() {
		volumeServer.Shutdown()
	})

	if e := http.Serve(listener, r); e != nil {
		glog.Fatalf("Fail to serve:%s", e.Error())
	}
	return true
}
