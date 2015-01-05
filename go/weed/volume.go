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
	volumeSecurePort      = cmdVolume.Flag.Int("port.secure", 8443, "https listen port, active when SSL certs are specified. Not ready yet.")
	volumeFolders         = cmdVolume.Flag.String("dir", os.TempDir(), "directories to store data files. dir[,dir]...")
	maxVolumeCounts       = cmdVolume.Flag.String("max", "7", "maximum numbers of volumes, count[,count]...")
	ip                    = cmdVolume.Flag.String("ip", "", "ip or server name")
	publicIp              = cmdVolume.Flag.String("publicIp", "", "Publicly accessible <ip|server_name>")
	volumeBindIp          = cmdVolume.Flag.String("ip.bind", "0.0.0.0", "ip address to bind to")
	masterNode            = cmdVolume.Flag.String("mserver", "localhost:9333", "master server location")
	vpulse                = cmdVolume.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats, must be smaller than or equal to the master's setting")
	vTimeout              = cmdVolume.Flag.Int("idleTimeout", 10, "connection idle seconds")
	vMaxCpu               = cmdVolume.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	dataCenter            = cmdVolume.Flag.String("dataCenter", "", "current volume server's data center name")
	rack                  = cmdVolume.Flag.String("rack", "", "current volume server's rack name")
	volumeWhiteListOption = cmdVolume.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	fixJpgOrientation     = cmdVolume.Flag.Bool("images.fix.orientation", true, "Adjust jpg orientation when uploading.")

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

	if *publicIp == "" {
		if *ip == "" {
			*publicIp = "localhost"
		} else {
			*publicIp = *ip
		}
	}
	if *volumeWhiteListOption != "" {
		volumeWhiteList = strings.Split(*volumeWhiteListOption, ",")
	}

	r := http.NewServeMux()

	volumeServer := weed_server.NewVolumeServer(r, *ip, *vport, *publicIp, folders, maxCounts,
		*masterNode, *vpulse, *dataCenter, *rack,
		volumeWhiteList,
		*fixJpgOrientation,
	)

	listeningAddress := *volumeBindIp + ":" + strconv.Itoa(*vport)

	glog.V(0).Infoln("Start Seaweed volume server", util.VERSION, "at", listeningAddress)

	listener, e := util.NewListener(listeningAddress, time.Duration(*vTimeout)*time.Second)
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
