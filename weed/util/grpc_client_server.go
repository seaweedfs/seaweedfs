package util

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	// cache grpc connections
	grpcClients     = make(map[string]*grpc.ClientConn)
	grpcClientsLock sync.Mutex
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

func WithCachedGrpcClient(fn func(*grpc.ClientConn) error, address string, opts ...grpc.DialOption) error {

	grpcClientsLock.Lock()

	existingConnection, found := grpcClients[address]
	if found {
		grpcClientsLock.Unlock()
		return fn(existingConnection)
	}

	grpcConnection, err := GrpcDial(address, opts...)
	if err != nil {
		grpcClientsLock.Unlock()
		return fmt.Errorf("fail to dial %s: %v", address, err)
	}

	grpcClients[address] = grpcConnection
	grpcClientsLock.Unlock()

	err = fn(grpcConnection)
	if err != nil {
		grpcClientsLock.Lock()
		delete(grpcClients, address)
		grpcClientsLock.Unlock()
	}

	return err
}
