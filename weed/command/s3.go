package command

import (
	"net/http"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/gorilla/mux"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/s3api"
)

var (
	s3options S3Options
)

type S3Options struct {
	filer            *string
	filerGrpcPort    *int
	filerBucketsPath *string
	port             *int
	domainName       *string
}

func init() {
	cmdS3.Run = runS3 // break init cycle
	s3options.filer = cmdS3.Flag.String("filer", "localhost:8888", "filer server address")
	s3options.filerGrpcPort = cmdS3.Flag.Int("filer.grpcPort", 0, "filer server grpc port, default to filer http port plus 10000")
	s3options.filerBucketsPath = cmdS3.Flag.String("filer.dir.buckets", "/s3buckets", "folder on filer to store all buckets")
	s3options.port = cmdS3.Flag.Int("port", 8333, "s3options server http listen port")
	s3options.domainName = cmdS3.Flag.String("domainName", "", "suffix of the host name, {bucket}.{domainName}")
}

var cmdS3 = &Command{
	UsageLine: "s3 -port=8333 -filer=<ip:port>",
	Short:     "start a s3 API compatible server that is backed by a filer",
	Long: `start a s3 API compatible server that is backed by a filer.

`,
}

func runS3(cmd *Command, args []string) bool {

	filerGrpcAddress, err := parseFilerGrpcAddress(*s3options.filer, *s3options.filerGrpcPort)
	if err != nil {
		glog.Fatal(err)
		return false
	}

	router := mux.NewRouter().SkipClean(true)

	_, s3ApiServer_err := s3api.NewS3ApiServer(router, &s3api.S3ApiServerOption{
		Filer:            *s3options.filer,
		FilerGrpcAddress: filerGrpcAddress,
		DomainName:       *s3options.domainName,
		BucketsPath:      *s3options.filerBucketsPath,
	})
	if s3ApiServer_err != nil {
		glog.Fatalf("S3 API Server startup error: %v", s3ApiServer_err)
	}

	glog.V(0).Infof("Start Seaweed S3 API Server %s at port %d", util.VERSION, *s3options.port)
	s3ApiListener, e := util.NewListener(fmt.Sprintf(":%d", *s3options.port), time.Duration(10)*time.Second)
	if e != nil {
		glog.Fatalf("S3 API Server listener error: %v", e)
	}

	httpS := &http.Server{Handler: router}
	if err := httpS.Serve(s3ApiListener); err != nil {
		glog.Fatalf("S3 API Server Fail to serve: %v", e)
	}

	return true

}
