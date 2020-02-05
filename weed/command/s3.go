package command

import (
	"fmt"
	"net/http"
	"time"

	"github.com/chrislusf/seaweedfs/weed/security"

	"github.com/gorilla/mux"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/s3api"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	s3StandaloneOptions S3Options
)

type S3Options struct {
	filer            *string
	filerBucketsPath *string
	port             *int
	domainName       *string
	tlsPrivateKey    *string
	tlsCertificate   *string
}

func init() {
	cmdS3.Run = runS3 // break init cycle
	s3StandaloneOptions.filer = cmdS3.Flag.String("filer", "localhost:8888", "filer server address")
	s3StandaloneOptions.filerBucketsPath = cmdS3.Flag.String("filer.dir.buckets", "/buckets", "folder on filer to store all buckets")
	s3StandaloneOptions.port = cmdS3.Flag.Int("port", 8333, "s3 server http listen port")
	s3StandaloneOptions.domainName = cmdS3.Flag.String("domainName", "", "suffix of the host name, {bucket}.{domainName}")
	s3StandaloneOptions.tlsPrivateKey = cmdS3.Flag.String("key.file", "", "path to the TLS private key file")
	s3StandaloneOptions.tlsCertificate = cmdS3.Flag.String("cert.file", "", "path to the TLS certificate file")
}

var cmdS3 = &Command{
	UsageLine: "s3 -port=8333 -filer=<ip:port>",
	Short:     "start a s3 API compatible server that is backed by a filer",
	Long: `start a s3 API compatible server that is backed by a filer.

`,
}

func runS3(cmd *Command, args []string) bool {

	util.LoadConfiguration("security", false)

	return s3StandaloneOptions.startS3Server()

}

func (s3opt *S3Options) startS3Server() bool {

	filerGrpcAddress, err := parseFilerGrpcAddress(*s3opt.filer)
	if err != nil {
		glog.Fatal(err)
		return false
	}

	router := mux.NewRouter().SkipClean(true)

	_, s3ApiServer_err := s3api.NewS3ApiServer(router, &s3api.S3ApiServerOption{
		Filer:            *s3opt.filer,
		FilerGrpcAddress: filerGrpcAddress,
		DomainName:       *s3opt.domainName,
		BucketsPath:      *s3opt.filerBucketsPath,
		GrpcDialOption:   security.LoadClientTLS(util.GetViper(), "grpc.client"),
	})
	if s3ApiServer_err != nil {
		glog.Fatalf("S3 API Server startup error: %v", s3ApiServer_err)
	}

	httpS := &http.Server{Handler: router}

	listenAddress := fmt.Sprintf(":%d", *s3opt.port)
	s3ApiListener, err := util.NewListener(listenAddress, time.Duration(10)*time.Second)
	if err != nil {
		glog.Fatalf("S3 API Server listener on %s error: %v", listenAddress, err)
	}

	if *s3opt.tlsPrivateKey != "" {
		glog.V(0).Infof("Start Seaweed S3 API Server %s at https port %d", util.VERSION, *s3opt.port)
		if err = httpS.ServeTLS(s3ApiListener, *s3opt.tlsCertificate, *s3opt.tlsPrivateKey); err != nil {
			glog.Fatalf("S3 API Server Fail to serve: %v", err)
		}
	} else {
		glog.V(0).Infof("Start Seaweed S3 API Server %s at http port %d", util.VERSION, *s3opt.port)
		if err = httpS.Serve(s3ApiListener); err != nil {
			glog.Fatalf("S3 API Server Fail to serve: %v", err)
		}
	}

	return true

}
