package util

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func NewGrpcServer() *grpc.Server {
	return grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{
		Time:    10 * time.Second, // wait time before ping if no activity
		Timeout: 20 * time.Second, // ping timeout
	}), grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime: 60 * time.Second, // min time a client should wait before sending a ping
	}))
}

func GrpcDial(address string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {

	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:    30 * time.Second, // client ping server if no activity for this long
		Timeout: 20 * time.Second,
	}))

	return grpc.Dial(address, opts...)
}
