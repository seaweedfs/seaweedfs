package command

import (
	"net/http"
	"strconv"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/filer2"
)

var (
	f FilerOptions
)

type FilerOptions struct {
	master                  *string
	ip                      *string
	port                    *int
	publicPort              *int
	collection              *string
	defaultReplicaPlacement *string
	redirectOnRead          *bool
	disableDirListing       *bool
	maxMB                   *int
	secretKey               *string
}

func init() {
	cmdFiler.Run = runFiler // break init cycle
	f.master = cmdFiler.Flag.String("master", "localhost:9333", "master server location")
	f.collection = cmdFiler.Flag.String("collection", "", "all data will be stored in this collection")
	f.ip = cmdFiler.Flag.String("ip", "", "filer server http listen ip address")
	f.port = cmdFiler.Flag.Int("port", 8888, "filer server http listen port")
	f.publicPort = cmdFiler.Flag.Int("port.public", 0, "port opened to public")
	f.defaultReplicaPlacement = cmdFiler.Flag.String("defaultReplicaPlacement", "000", "default replication type if not specified")
	f.redirectOnRead = cmdFiler.Flag.Bool("redirectOnRead", false, "whether proxy or redirect to volume server during file GET request")
	f.disableDirListing = cmdFiler.Flag.Bool("disableDirListing", false, "turn off directory listing")
	f.maxMB = cmdFiler.Flag.Int("maxMB", 32, "split files larger than the limit")
	f.secretKey = cmdFiler.Flag.String("secure.secret", "", "secret to encrypt Json Web Token(JWT)")
}

var cmdFiler = &Command{
	UsageLine: "filer -port=8888 -master=<ip:port>",
	Short:     "start a file server that points to a master server",
	Long: `start a file server which accepts REST operation for any files.

	//create or overwrite the file, the directories /path/to will be automatically created
	POST /path/to/file
	//get the file content
	GET /path/to/file
	//create or overwrite the file, the filename in the multipart request will be used
	POST /path/to/
	//return a json format subdirectory and files listing
	GET /path/to/

	The configuration file "filer.toml" is read from ".", "$HOME/.seaweedfs/", or "/etc/seaweedfs/", in that order.

	The following are example filer.toml configuration file.

` + filer2.FILER_TOML_EXAMPLE + "\n",
}

func runFiler(cmd *Command, args []string) bool {

	f.start()

	return true
}

func (fo *FilerOptions) start() {

	defaultMux := http.NewServeMux()
	publicVolumeMux := defaultMux

	if *fo.publicPort != 0 {
		publicVolumeMux = http.NewServeMux()
	}

	fs, nfs_err := weed_server.NewFilerServer(defaultMux, publicVolumeMux,
		*fo.ip, *fo.port, *fo.master, *fo.collection,
		*fo.defaultReplicaPlacement, *fo.redirectOnRead, *fo.disableDirListing,
		*fo.maxMB,
		*fo.secretKey,
	)
	if nfs_err != nil {
		glog.Fatalf("Filer startup error: %v", nfs_err)
	}

	if *fo.publicPort != 0 {
		publicListeningAddress := *fo.ip + ":" + strconv.Itoa(*fo.publicPort)
		glog.V(0).Infoln("Start Seaweed filer server", util.VERSION, "public at", publicListeningAddress)
		publicListener, e := util.NewListener(publicListeningAddress, 0)
		if e != nil {
			glog.Fatalf("Filer server public listener error on port %d:%v", *fo.publicPort, e)
		}
		go func() {
			if e := http.Serve(publicListener, publicVolumeMux); e != nil {
				glog.Fatalf("Volume server fail to serve public: %v", e)
			}
		}()
	}

	glog.V(0).Infoln("Start Seaweed Filer", util.VERSION, "at port", strconv.Itoa(*fo.port))
	filerListener, e := util.NewListener(
		":"+strconv.Itoa(*fo.port),
		time.Duration(10)*time.Second,
	)
	if e != nil {
		glog.Fatalf("Filer listener error: %v", e)
	}

	m := cmux.New(filerListener)
	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.Any())

	// Create your protocol servers.
	grpcS := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcS, fs)
	reflection.Register(grpcS)

	httpS := &http.Server{Handler: defaultMux}

	go grpcS.Serve(grpcL)
	go httpS.Serve(httpL)

	if err := m.Serve(); err != nil {
		glog.Fatalf("Filer Fail to serve: %v", e)
	}

}
