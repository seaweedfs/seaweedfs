package main

// run: go run test/grpc/master/master-grpc-client.go

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/credentials/insecure"
)

var masterAddress = ""
var collection = ""

var client master_pb.SeaweedClient

func main() {
	if masterAddress == "" || collection == "" {
		glog.Errorf("please config masterAddress and collection before run test")
		return
	}

	initConn()

	testCollectionMark()

	glog.Info("test done and all passed")
}

func initConn() {

	opts := []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"LoadBalancingPolicy": "%s"}`, roundrobin.Name)),
	}

	// log.Info().Str("target", target).Msg("[msc] initializing gRPC client by K8SURL")
	conn, err := grpc.DialContext(context.Background(), masterAddress, opts...)
	if err != nil {
		glog.Errorf("master grpc client init failed %s %v", masterAddress, err)
		return
	}
	client = master_pb.NewSeaweedClient(conn)
}

func testCollectionMark() {
	_, err := client.CollectionMark(context.Background(), &master_pb.CollectionMarkRequest{
		Name: collection,
		Flag: "readonly",
	})
	if err != nil {
		panic(err)
	}

	_, err = client.CollectionMark(context.Background(), &master_pb.CollectionMarkRequest{
		Name: collection,
		Flag: "writable",
	})
	if err != nil {
		panic(err)
	}

	_, err = client.CollectionMark(context.Background(), &master_pb.CollectionMarkRequest{
		Name: collection,
		Flag: "not-support",
	})
	if err == nil {
		panic(fmt.Errorf("should failed"))
	}
}
