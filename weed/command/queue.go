package command

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/grpc/reflection"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/queue_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	weed_server "github.com/chrislusf/seaweedfs/weed/server"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	queueStandaloneOptions QueueOptions
)

type QueueOptions struct {
	filer          *string
	port           *int
	tlsPrivateKey  *string
	tlsCertificate *string
	defaultTtl     *string
}

func init() {
	cmdQueue.Run = runQueue // break init cycle
	queueStandaloneOptions.filer = cmdQueue.Flag.String("filer", "localhost:8888", "filer server address")
	queueStandaloneOptions.port = cmdQueue.Flag.Int("port", 17777, "queue server gRPC listen port")
	queueStandaloneOptions.tlsPrivateKey = cmdQueue.Flag.String("key.file", "", "path to the TLS private key file")
	queueStandaloneOptions.tlsCertificate = cmdQueue.Flag.String("cert.file", "", "path to the TLS certificate file")
	queueStandaloneOptions.defaultTtl = cmdQueue.Flag.String("ttl", "1h", "time to live, e.g.: 1m, 1h, 1d, 1M, 1y")
}

var cmdQueue = &Command{
	UsageLine: "<WIP> queue [-port=17777] [-filer=<ip:port>]",
	Short:     "start a queue gRPC server that is backed by a filer",
	Long: `start a queue gRPC server that is backed by a filer.

`,
}

func runQueue(cmd *Command, args []string) bool {

	util.LoadConfiguration("security", false)

	return queueStandaloneOptions.startQueueServer()

}

func (queueopt *QueueOptions) startQueueServer() bool {

	filerGrpcAddress, err := parseFilerGrpcAddress(*queueopt.filer)
	if err != nil {
		glog.Fatal(err)
		return false
	}

	filerQueuesPath := "/queues"

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	for {
		err = withFilerClient(filerGrpcAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get filer %s configuration: %v", filerGrpcAddress, err)
			}
			filerQueuesPath = resp.DirQueues
			glog.V(0).Infof("Queue read filer queues dir: %s", filerQueuesPath)
			return nil
		})
		if err != nil {
			glog.V(0).Infof("wait to connect to filer %s grpc address %s", *queueopt.filer, filerGrpcAddress)
			time.Sleep(time.Second)
		} else {
			glog.V(0).Infof("connected to filer %s grpc address %s", *queueopt.filer, filerGrpcAddress)
			break
		}
	}

	qs, err := weed_server.NewQueueServer(&weed_server.QueueServerOption{
		Filers:             []string{*queueopt.filer},
		DefaultReplication: "",
		MaxMB:              0,
		Port:               *queueopt.port,
	})

	// start grpc listener
	grpcL, err := util.NewListener(":"+strconv.Itoa(*queueopt.port), 0)
	if err != nil {
		glog.Fatalf("failed to listen on grpc port %d: %v", *queueopt.port, err)
	}
	grpcS := util.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.queue"))
	queue_pb.RegisterSeaweedQueueServer(grpcS, qs)
	reflection.Register(grpcS)
	go grpcS.Serve(grpcL)

	return true

}
