package command

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	gatewayOptions GatewayOptions
)

type GatewayOptions struct {
	masters *string
	filers  *string
	bindIp  *string
	port    *int
	maxMB   *int
}

func init() {
	cmdGateway.Run = runGateway // break init cycle
	gatewayOptions.masters = cmdGateway.Flag.String("master", "localhost:9333", "comma-separated master servers")
	gatewayOptions.filers = cmdGateway.Flag.String("filer", "localhost:8888", "comma-separated filer servers")
	gatewayOptions.bindIp = cmdGateway.Flag.String("ip.bind", "localhost", "ip address to bind to")
	gatewayOptions.port = cmdGateway.Flag.Int("port", 5647, "gateway http listen port")
	gatewayOptions.maxMB = cmdGateway.Flag.Int("maxMB", 4, "split files larger than the limit")
}

var cmdGateway = &Command{
	UsageLine: "gateway -port=8888 -master=<ip:port>[,<ip:port>]* -filer=<ip:port>[,<ip:port>]*",
	Short:     "start a gateway server that points to a list of master servers or a list of filers",
	Long: `start a gateway server which accepts REST operation to write any blobs, files, or topic messages.

	POST /blobs/
		upload the blob and return a chunk id
	DELETE /blobs/<chunk_id>
		delete a chunk id

	/*
	POST /files/path/to/a/file
		save /path/to/a/file on filer 
	DELETE /files/path/to/a/file
		delete /path/to/a/file on filer 

	POST /topics/topicName
		save on filer to /topics/topicName/<ds>/ts.json
	*/
`,
}

func runGateway(cmd *Command, args []string) bool {

	util.LoadConfiguration("security", false)

	gatewayOptions.startGateway()

	return true
}

func (gw *GatewayOptions) startGateway() {

	defaultMux := http.NewServeMux()

	_, gws_err := weed_server.NewGatewayServer(defaultMux, &weed_server.GatewayOption{
		Masters: strings.Split(*gw.masters, ","),
		Filers:  strings.Split(*gw.filers, ","),
		MaxMB:   *gw.maxMB,
	})
	if gws_err != nil {
		glog.Fatalf("Gateway startup error: %v", gws_err)
	}

	glog.V(0).Infof("Start Seaweed Gateway %s at %s:%d", util.Version(), *gw.bindIp, *gw.port)
	gatewayListener, e := util.NewListener(
		*gw.bindIp+":"+strconv.Itoa(*gw.port),
		time.Duration(10)*time.Second,
	)
	if e != nil {
		glog.Fatalf("Filer listener error: %v", e)
	}

	httpS := &http.Server{Handler: defaultMux}
	if err := httpS.Serve(gatewayListener); err != nil {
		glog.Fatalf("Gateway Fail to serve: %v", e)
	}

}
