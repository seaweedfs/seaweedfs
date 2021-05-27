package pb

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"math/rand"
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
	grpcClients     = make(map[string]*versionedGrpcClient)
	grpcClientsLock sync.Mutex
)

type versionedGrpcClient struct {
	*grpc.ClientConn
	version int
}

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

func getOrCreateConnection(address string, opts ...grpc.DialOption) (*versionedGrpcClient, error) {

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

	vgc := &versionedGrpcClient{
		grpcConnection,
		rand.Int(),
	}
	grpcClients[address] = vgc

	return vgc, nil
}

func WithCachedGrpcClient(fn func(*grpc.ClientConn) error, address string, opts ...grpc.DialOption) error {

	vgc, err := getOrCreateConnection(address, opts...)
	if err != nil {
		return fmt.Errorf("getOrCreateConnection %s: %v", address, err)
	}
	executionErr := fn(vgc.ClientConn)
	if executionErr != nil && strings.Contains(executionErr.Error(), "transport") {
		grpcClientsLock.Lock()
		if t, ok := grpcClients[address]; ok {
			if t.version == vgc.version {
				vgc.Close()
				delete(grpcClients, address)
			}
		}
		grpcClientsLock.Unlock()
	}

	return executionErr
}

func ParseServerToGrpcAddress(server string) (serverGrpcAddress string, err error) {
	return ParseServerAddress(server, 10000)
}
func ParseServersToGrpcAddresses(servers []string) (serverGrpcAddresses []string, err error) {
	for _, server := range servers {
		if serverGrpcAddress, parseErr := ParseServerToGrpcAddress(server); parseErr == nil {
			serverGrpcAddresses = append(serverGrpcAddresses, serverGrpcAddress)
		} else {
			return nil, parseErr
		}
	}
	return
}

func ParseServerAddress(server string, deltaPort int) (newServerAddress string, err error) {

	host, port, parseErr := hostAndPort(server)
	if parseErr != nil {
		return "", fmt.Errorf("server port parse error: %v", parseErr)
	}

	newPort := int(port) + deltaPort

	return fmt.Sprintf("%s:%d", host, newPort), nil
}

func hostAndPort(address string) (host string, port uint64, err error) {
	colonIndex := strings.LastIndex(address, ":")
	if colonIndex < 0 {
		return "", 0, fmt.Errorf("server should have hostname:port format: %v", address)
	}
	port, err = strconv.ParseUint(address[colonIndex+1:], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("server port parse error: %v", err)
	}

	return address[:colonIndex], port, err
}

func ServerToGrpcAddress(server string) (serverGrpcAddress string) {

	host, port, parseErr := hostAndPort(server)
	if parseErr != nil {
		glog.Fatalf("server address %s parse error: %v", server, parseErr)
	}

	grpcPort := int(port) + 10000

	return fmt.Sprintf("%s:%d", host, grpcPort)
}

func GrpcAddressToServerAddress(grpcAddress string) (serverAddress string) {
	host, grpcPort, parseErr := hostAndPort(grpcAddress)
	if parseErr != nil {
		glog.Fatalf("server grpc address %s parse error: %v", grpcAddress, parseErr)
	}

	port := int(grpcPort) - 10000

	return fmt.Sprintf("%s:%d", host, port)
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

func WithOneOfGrpcFilerClients(filerGrpcAddresses []string, grpcDialOption grpc.DialOption, fn func(client filer_pb.SeaweedFilerClient) error) (err error) {

	for _, filerGrpcAddress := range filerGrpcAddresses {
		err = WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
			client := filer_pb.NewSeaweedFilerClient(grpcConnection)
			return fn(client)
		}, filerGrpcAddress, grpcDialOption)
		if err == nil {
			return nil
		}
	}

	return err
}
