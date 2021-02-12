package pb

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

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

const (
	Max_Message_Size = 1 << 30 // 1 GB
)

var (
	// cache grpc connections
	grpcClients     = make(map[string]*grpc.ClientConn)
	grpcClientsLock sync.Mutex
)

func init() {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1024
	http.DefaultTransport.(*http.Transport).MaxIdleConns = 1024
}

func NewGrpcServer(opts ...grpc.ServerOption) *grpc.Server {
	var options []grpc.ServerOption
	options = append(options,
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:             10 * time.Second, // wait time before ping if no activity
			Timeout:          20 * time.Second, // ping timeout
			MaxConnectionAge: 10 * time.Hour,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             60 * time.Second, // min time a client should wait before sending a ping
			PermitWithoutStream: false,
		}),
		grpc.MaxRecvMsgSize(Max_Message_Size),
		grpc.MaxSendMsgSize(Max_Message_Size),
	)
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
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(Max_Message_Size),
			grpc.MaxCallRecvMsgSize(Max_Message_Size),
			grpc.WaitForReady(true),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second, // client ping server if no activity for this long
			Timeout:             20 * time.Second,
			PermitWithoutStream: false,
		}))
	for _, opt := range opts {
		if opt != nil {
			options = append(options, opt)
		}
	}
	return grpc.DialContext(ctx, address, options...)
}

func getOrCreateConnection(address string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {

	grpcClientsLock.Lock()
	defer grpcClientsLock.Unlock()

	existingConnection, found := grpcClients[address]
	if found {
		return existingConnection, nil
	}

	grpcConnection, err := GrpcDial(context.Background(), address, opts...)
	if err != nil {
		return nil, fmt.Errorf("fail to dial %s: %v", address, err)
	}

	grpcClients[address] = grpcConnection

	return grpcConnection, nil
}

func WithCachedGrpcClient(fn func(*grpc.ClientConn) error, address string, opts ...grpc.DialOption) error {

	grpcConnection, err := getOrCreateConnection(address, opts...)
	if err != nil {
		return fmt.Errorf("getOrCreateConnection %s: %v", address, err)
	}
	return fn(grpcConnection)
}

func ParseServerToGrpcAddress(server string) (serverGrpcAddress string, err error) {
	colonIndex := strings.LastIndex(server, ":")
	if colonIndex < 0 {
		return "", fmt.Errorf("server should have hostname:port format: %v", server)
	}

	port, parseErr := strconv.ParseUint(server[colonIndex+1:], 10, 64)
	if parseErr != nil {
		return "", fmt.Errorf("server port parse error: %v", parseErr)
	}

	grpcPort := int(port) + 10000

	return fmt.Sprintf("%s:%d", server[:colonIndex], grpcPort), nil
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

func GrpcAddressToServerAddress(grpcAddress string) (serverAddress string) {
	hostnameAndPort := strings.Split(grpcAddress, ":")
	if len(hostnameAndPort) != 2 {
		return fmt.Sprintf("unexpected grpcAddress: %s", grpcAddress)
	}

	grpcPort, parseErr := strconv.ParseUint(hostnameAndPort[1], 10, 64)
	if parseErr != nil {
		return fmt.Sprintf("failed to parse port for %s:%s", hostnameAndPort[0], hostnameAndPort[1])
	}

	port := int(grpcPort) - 10000

	return fmt.Sprintf("%s:%d", hostnameAndPort[0], port)
}

func WithMasterClient(master string, grpcDialOption grpc.DialOption, fn func(client master_pb.SeaweedClient) error) error {

	masterGrpcAddress, parseErr := ParseServerToGrpcAddress(master)
	if parseErr != nil {
		return fmt.Errorf("failed to parse master grpc %v: %v", master, parseErr)
	}

	return WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
		client := master_pb.NewSeaweedClient(grpcConnection)
		return fn(client)
	}, masterGrpcAddress, grpcDialOption)

}

func WithBrokerGrpcClient(brokerGrpcAddress string, grpcDialOption grpc.DialOption, fn func(client messaging_pb.SeaweedMessagingClient) error) error {

	return WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
		client := messaging_pb.NewSeaweedMessagingClient(grpcConnection)
		return fn(client)
	}, brokerGrpcAddress, grpcDialOption)

}

func WithFilerClient(filer string, grpcDialOption grpc.DialOption, fn func(client filer_pb.SeaweedFilerClient) error) error {

	filerGrpcAddress, parseErr := ParseServerToGrpcAddress(filer)
	if parseErr != nil {
		return fmt.Errorf("failed to parse filer grpc %v: %v", filer, parseErr)
	}

	return WithGrpcFilerClient(filerGrpcAddress, grpcDialOption, fn)

}

func WithGrpcFilerClient(filerGrpcAddress string, grpcDialOption grpc.DialOption, fn func(client filer_pb.SeaweedFilerClient) error) error {

	return WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, filerGrpcAddress, grpcDialOption)

}

func ParseFilerGrpcAddress(filer string) (filerGrpcAddress string, err error) {
	hostnameAndPort := strings.Split(filer, ":")
	if len(hostnameAndPort) != 2 {
		return "", fmt.Errorf("filer should have hostname:port format: %v", hostnameAndPort)
	}

	filerPort, parseErr := strconv.ParseUint(hostnameAndPort[1], 10, 64)
	if parseErr != nil {
		return "", fmt.Errorf("filer port parse error: %v", parseErr)
	}

	filerGrpcPort := int(filerPort) + 10000

	return fmt.Sprintf("%s:%d", hostnameAndPort[0], filerGrpcPort), nil
}
