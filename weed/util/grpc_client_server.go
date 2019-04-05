package util

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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

func init(){
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100
}

func NewGrpcServer(opts ...grpc.ServerOption) *grpc.Server {
	var options []grpc.ServerOption
	options = append(options, grpc.KeepaliveParams(keepalive.ServerParameters{
		Time:    10 * time.Second, // wait time before ping if no activity
		Timeout: 20 * time.Second, // ping timeout
	}), grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime: 60 * time.Second, // min time a client should wait before sending a ping
	}))
	for _, opt := range opts {
		if opt != nil {
			options = append(options, opt)
		}
	}
	return grpc.NewServer(options...)
}

func GrpcDial(ctx context.Context, address string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// opts = append(opts, grpc.WithBlock())
	// opts = append(opts, grpc.WithTimeout(time.Duration(5*time.Second)))
	var options []grpc.DialOption
	options = append(options,
		// grpc.WithInsecure(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    30 * time.Second, // client ping server if no activity for this long
			Timeout: 20 * time.Second,
		}))
	for _, opt := range opts {
		if opt != nil {
			options = append(options, opt)
		}
	}
	return grpc.DialContext(ctx, address, options...)
}

func WithCachedGrpcClient(ctx context.Context, fn func(*grpc.ClientConn) error, address string, opts ...grpc.DialOption) error {

	grpcClientsLock.Lock()

	existingConnection, found := grpcClients[address]
	if found {
		grpcClientsLock.Unlock()
		return fn(existingConnection)
	}

	grpcConnection, err := GrpcDial(ctx, address, opts...)
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

func ParseServerToGrpcAddress(server string) (serverGrpcAddress string, err error) {
	hostnameAndPort := strings.Split(server, ":")
	if len(hostnameAndPort) != 2 {
		return "", fmt.Errorf("server should have hostname:port format: %v", hostnameAndPort)
	}

	port, parseErr := strconv.ParseUint(hostnameAndPort[1], 10, 64)
	if parseErr != nil {
		return "", fmt.Errorf("server port parse error: %v", parseErr)
	}

	grpcPort := int(port) + 10000

	return fmt.Sprintf("%s:%d", hostnameAndPort[0], grpcPort), nil
}

func ServerToGrpcAddress(server string) (serverGrpcAddress string) {
	hostnameAndPort := strings.Split(server, ":")
	if len(hostnameAndPort) != 2 {
		return fmt.Sprintf("unexpected server address: %s", server)
	}

	port, parseErr := strconv.ParseUint(hostnameAndPort[1], 10, 64)
	if parseErr != nil {
		return fmt.Sprintf("failed to parse port for %s:%s", hostnameAndPort[0], hostnameAndPort[1])
	}

	grpcPort := int(port) + 10000

	return fmt.Sprintf("%s:%d", hostnameAndPort[0], grpcPort)
}
