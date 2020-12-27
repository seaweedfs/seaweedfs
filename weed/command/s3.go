package command

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"

	"github.com/gorilla/mux"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/s3api"
	stats_collect "github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	s3StandaloneOptions S3Options
)

type S3Options struct {
	filer            *string
	port             *int
	config           *string
	domainName       *string
	tlsPrivateKey    *string
	tlsCertificate   *string
	metricsHttpPort  *int
	allowEmptyFolder *bool
}

func init() {
	cmdS3.Run = runS3 // break init cycle
	s3StandaloneOptions.filer = cmdS3.Flag.String("filer", "localhost:8888", "filer server address")
	s3StandaloneOptions.port = cmdS3.Flag.Int("port", 8333, "s3 server http listen port")
	s3StandaloneOptions.domainName = cmdS3.Flag.String("domainName", "", "suffix of the host name in comma separated list, {bucket}.{domainName}")
	s3StandaloneOptions.config = cmdS3.Flag.String("config", "", "path to the config file")
	s3StandaloneOptions.tlsPrivateKey = cmdS3.Flag.String("key.file", "", "path to the TLS private key file")
	s3StandaloneOptions.tlsCertificate = cmdS3.Flag.String("cert.file", "", "path to the TLS certificate file")
	s3StandaloneOptions.metricsHttpPort = cmdS3.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	s3StandaloneOptions.allowEmptyFolder = cmdS3.Flag.Bool("allowEmptyFolder", false, "allow empty folders")
}

var cmdS3 = &Command{
	UsageLine: "s3 [-port=8333] [-filer=<ip:port>] [-config=</path/to/config.json>]",
	Short:     "start a s3 API compatible server that is backed by a filer",
	Long: `start a s3 API compatible server that is backed by a filer.

	By default, you can use any access key and secret key to access the S3 APIs.
	To enable credential based access, create a config.json file similar to this:

{
  "identities": [
    {
      "name": "anonymous",
      "actions": [
        "Read"
      ]
    },
    {
      "name": "some_admin_user",
      "credentials": [
        {
          "accessKey": "some_access_key1",
          "secretKey": "some_secret_key1"
        }
      ],
      "actions": [
        "Admin",
        "Read",
        "List",
        "Tagging",
        "Write"
      ]
    },
    {
      "name": "some_read_only_user",
      "credentials": [
        {
          "accessKey": "some_access_key2",
          "secretKey": "some_secret_key2"
        }
      ],
      "actions": [
        "Read"
      ]
    },
    {
      "name": "some_normal_user",
      "credentials": [
        {
          "accessKey": "some_access_key3",
          "secretKey": "some_secret_key3"
        }
      ],
      "actions": [
        "Read",
        "List",
        "Tagging",
        "Write"
      ]
    },
    {
      "name": "user_limited_to_bucket1",
      "credentials": [
        {
          "accessKey": "some_access_key4",
          "secretKey": "some_secret_key4"
        }
      ],
      "actions": [
        "Read:bucket1",
        "List:bucket1",
        "Tagging:bucket1",
        "Write:bucket1"
      ]
    }
  ]
}

`,
}

func runS3(cmd *Command, args []string) bool {

	util.LoadConfiguration("security", false)

	go stats_collect.StartMetricsServer(*s3StandaloneOptions.metricsHttpPort)

	return s3StandaloneOptions.startS3Server()

}

func (s3opt *S3Options) startS3Server() bool {

	filerGrpcAddress, err := pb.ParseFilerGrpcAddress(*s3opt.filer)
	if err != nil {
		glog.Fatal(err)
		return false
	}

	filerBucketsPath := "/buckets"

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	// metrics read from the filer
	var metricsAddress string
	var metricsIntervalSec int

	for {
		err = pb.WithGrpcFilerClient(filerGrpcAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get filer %s configuration: %v", filerGrpcAddress, err)
			}
			filerBucketsPath = resp.DirBuckets
			metricsAddress, metricsIntervalSec = resp.MetricsAddress, int(resp.MetricsIntervalSec)
			glog.V(0).Infof("S3 read filer buckets dir: %s", filerBucketsPath)
			return nil
		})
		if err != nil {
			glog.V(0).Infof("wait to connect to filer %s grpc address %s", *s3opt.filer, filerGrpcAddress)
			time.Sleep(time.Second)
		} else {
			glog.V(0).Infof("connected to filer %s grpc address %s", *s3opt.filer, filerGrpcAddress)
			break
		}
	}

	go stats_collect.LoopPushingMetric("s3", stats_collect.SourceName(uint32(*s3opt.port)), metricsAddress, metricsIntervalSec)

	router := mux.NewRouter().SkipClean(true)

	_, s3ApiServer_err := s3api.NewS3ApiServer(router, &s3api.S3ApiServerOption{
		Filer:            *s3opt.filer,
		Port:             *s3opt.port,
		FilerGrpcAddress: filerGrpcAddress,
		Config:           *s3opt.config,
		DomainName:       *s3opt.domainName,
		BucketsPath:      filerBucketsPath,
		GrpcDialOption:   grpcDialOption,
		AllowEmptyFolder: *s3opt.allowEmptyFolder,
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
		glog.V(0).Infof("Start Seaweed S3 API Server %s at https port %d", util.Version(), *s3opt.port)
		if err = httpS.ServeTLS(s3ApiListener, *s3opt.tlsCertificate, *s3opt.tlsPrivateKey); err != nil {
			glog.Fatalf("S3 API Server Fail to serve: %v", err)
		}
	} else {
		glog.V(0).Infof("Start Seaweed S3 API Server %s at http port %d", util.Version(), *s3opt.port)
		if err = httpS.Serve(s3ApiListener); err != nil {
			glog.Fatalf("S3 API Server Fail to serve: %v", err)
		}
	}

	return true

}
