package operation

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	connectionPool     = make(map[string]*sync.Pool)
	connectionPoolLock sync.Mutex
)

func WithVolumeServerClient(volumeServer string, grpcDialOption grpc.DialOption, fn func(context.Context, volume_server_pb.VolumeServerClient) error) error {

	ctx := context.Background()

	grpcAddress, err := toVolumeServerGrpcAddress(volumeServer)
	if err != nil {
		return err
	}

	return util.WithCachedGrpcClient(ctx, func(ctx2 context.Context, grpcConnection *grpc.ClientConn) error {
		client := volume_server_pb.NewVolumeServerClient(grpcConnection)
		return fn(ctx2, client)
	}, grpcAddress, grpcDialOption)

}

func toVolumeServerGrpcAddress(volumeServer string) (grpcAddress string, err error) {
	sepIndex := strings.LastIndex(volumeServer, ":")
	port, err := strconv.Atoi(volumeServer[sepIndex+1:])
	if err != nil {
		glog.Errorf("failed to parse volume server address: %v", volumeServer)
		return "", err
	}
	return fmt.Sprintf("%s:%d", volumeServer[0:sepIndex], port+10000), nil
}

func WithVolumeServerTcpConnection(volumeServer string, fn func(conn net.Conn) error) error {
	tcpAddress, err := toVolumeServerTcpAddress(volumeServer)
	if err != nil {
		return err
	}

	conn := getConnection(tcpAddress)
	defer releaseConnection(conn, tcpAddress)

	err = fn(conn)
	return err
}

func getConnection(tcpAddress string) net.Conn {
	connectionPoolLock.Lock()
	defer connectionPoolLock.Unlock()

	pool, found := connectionPool[tcpAddress]
	if !found {
		pool = &sync.Pool{New: func() interface{} {
			conn, err := net.Dial("tcp", tcpAddress)
			if err != nil {
				glog.Errorf("failed to connect to %s: %v", tcpAddress, err)
				return conn
			}
			// println("connected", tcpAddress, "=>", conn.LocalAddr().String())
			return conn
		}}
		connectionPool[tcpAddress] = pool
	}
	conn := pool.Get().(net.Conn)
	// println("get connection", tcpAddress, "=>", conn.LocalAddr().String())
	return conn
}

func releaseConnection(conn net.Conn, tcpAddress string) {
	connectionPoolLock.Lock()
	defer connectionPoolLock.Unlock()

	pool, found := connectionPool[tcpAddress]
	if !found {
		// println("can not return connection", tcpAddress, "=>", conn.LocalAddr().String())
		return
	}
	pool.Put(conn)
	// println("returned connection", tcpAddress, "=>", conn.LocalAddr().String())
}

func toVolumeServerTcpAddress(volumeServer string) (grpcAddress string, err error) {
	sepIndex := strings.LastIndex(volumeServer, ":")
	port, err := strconv.Atoi(volumeServer[sepIndex+1:])
	if err != nil {
		glog.Errorf("failed to parse volume server address: %v", volumeServer)
		return "", err
	}
	return fmt.Sprintf("%s:%d", volumeServer[0:sepIndex], port+20000), nil
}

func WithMasterServerClient(masterServer string, grpcDialOption grpc.DialOption, fn func(ctx2 context.Context, masterClient master_pb.SeaweedClient) error) error {

	ctx := context.Background()

	masterGrpcAddress, parseErr := util.ParseServerToGrpcAddress(masterServer)
	if parseErr != nil {
		return fmt.Errorf("failed to parse master grpc %v: %v", masterServer, parseErr)
	}

	return util.WithCachedGrpcClient(ctx, func(ctx2 context.Context, grpcConnection *grpc.ClientConn) error {
		client := master_pb.NewSeaweedClient(grpcConnection)
		return fn(ctx2, client)
	}, masterGrpcAddress, grpcDialOption)

}
