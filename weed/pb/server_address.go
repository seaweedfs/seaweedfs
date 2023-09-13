package pb

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"net"
	"strconv"
	"strings"
)

type ServerAddress string
type ServerAddresses string
type ServerSrvAddress string

func NewServerAddress(host string, port int, grpcPort int) ServerAddress {
	if grpcPort == 0 || grpcPort == port+10000 {
		return ServerAddress(util.JoinHostPort(host, port))
	}
	return ServerAddress(util.JoinHostPort(host, port) + "." + strconv.Itoa(grpcPort))
}

func NewServerAddressWithGrpcPort(address string, grpcPort int) ServerAddress {
	if grpcPort == 0 {
		return ServerAddress(address)
	}
	_, port, _ := hostAndPort(address)
	if uint64(grpcPort) == port+10000 {
		return ServerAddress(address)
	}
	return ServerAddress(address + "." + strconv.Itoa(grpcPort))
}

func NewServerAddressFromDataNode(dn *master_pb.DataNodeInfo) ServerAddress {
	return NewServerAddressWithGrpcPort(dn.Id, int(dn.GrpcPort))
}

func NewServerAddressFromLocation(dn *master_pb.Location) ServerAddress {
	return NewServerAddressWithGrpcPort(dn.Url, int(dn.GrpcPort))
}

func (sa ServerAddress) String() string {
	return sa.ToHttpAddress()
}

func (sa ServerAddress) ToHttpAddress() string {
	portsSepIndex := strings.LastIndex(string(sa), ":")
	if portsSepIndex < 0 {
		return string(sa)
	}
	if portsSepIndex+1 >= len(sa) {
		return string(sa)
	}
	ports := string(sa[portsSepIndex+1:])
	sepIndex := strings.LastIndex(string(ports), ".")
	if sepIndex >= 0 {
		host := string(sa[0:portsSepIndex])
		return net.JoinHostPort(host, ports[0:sepIndex])
	}
	return string(sa)
}

func (sa ServerAddress) ToGrpcAddress() string {
	portsSepIndex := strings.LastIndex(string(sa), ":")
	if portsSepIndex < 0 {
		return string(sa)
	}
	if portsSepIndex+1 >= len(sa) {
		return string(sa)
	}
	ports := string(sa[portsSepIndex+1:])
	sepIndex := strings.LastIndex(ports, ".")
	if sepIndex >= 0 {
		host := string(sa[0:portsSepIndex])
		return net.JoinHostPort(host, ports[sepIndex+1:])
	}
	return ServerToGrpcAddress(string(sa))
}

// LookUp may return an error for some records along with successful lookups - make sure you do not
// discard `addresses` even if `err == nil`
func (r ServerSrvAddress) LookUp() (addresses []ServerAddress, err error) {
	_, records, lookupErr := net.LookupSRV("", "", string(r))
	if lookupErr != nil {
		err = fmt.Errorf("lookup SRV address %s: %v", r, lookupErr)
	}
	for _, srv := range records {
		address := fmt.Sprintf("%s:%d", srv.Target, srv.Port)
		addresses = append(addresses, ServerAddress(address))
	}
	return
}

// ToServiceDiscovery expects one of: a comma-separated list of ip:port, like
//
//	10.0.0.1:9999,10.0.0.2:24:9999
//
// OR an SRV Record prepended with 'dnssrv+', like:
//
//	dnssrv+_grpc._tcp.master.consul
//	dnssrv+_grpc._tcp.headless.default.svc.cluster.local
//	dnssrv+seaweed-master.master.consul
func (sa ServerAddresses) ToServiceDiscovery() (sd *ServerDiscovery) {
	sd = &ServerDiscovery{}
	prefix := "dnssrv+"
	if strings.HasPrefix(string(sa), prefix) {
		trimmed := strings.TrimPrefix(string(sa), prefix)
		srv := ServerSrvAddress(trimmed)
		sd.srvRecord = &srv
	} else {
		sd.list = sa.ToAddresses()
	}
	return
}

func (sa ServerAddresses) ToAddresses() (addresses []ServerAddress) {
	parts := strings.Split(string(sa), ",")
	for _, address := range parts {
		if address != "" {
			addresses = append(addresses, ServerAddress(address))
		}
	}
	return
}

func (sa ServerAddresses) ToAddressMap() (addresses map[string]ServerAddress) {
	addresses = make(map[string]ServerAddress)
	for _, address := range sa.ToAddresses() {
		addresses[string(address)] = address
	}
	return
}

func (sa ServerAddresses) ToAddressStrings() (addresses []string) {
	parts := strings.Split(string(sa), ",")
	for _, address := range parts {
		addresses = append(addresses, address)
	}
	return
}

func ToAddressStrings(addresses []ServerAddress) []string {
	var strings []string
	for _, addr := range addresses {
		strings = append(strings, string(addr))
	}
	return strings
}
func ToAddressStringsFromMap(addresses map[string]ServerAddress) []string {
	var strings []string
	for _, addr := range addresses {
		strings = append(strings, string(addr))
	}
	return strings
}
func FromAddressStrings(strings []string) []ServerAddress {
	var addresses []ServerAddress
	for _, addr := range strings {
		addresses = append(addresses, ServerAddress(addr))
	}
	return addresses
}

func ParseUrl(input string) (address ServerAddress, path string, err error) {
	if !strings.HasPrefix(input, "http://") {
		return "", "", fmt.Errorf("url %s needs prefix 'http://'", input)
	}
	input = input[7:]
	pathSeparatorIndex := strings.Index(input, "/")
	hostAndPorts := input
	if pathSeparatorIndex > 0 {
		path = input[pathSeparatorIndex:]
		hostAndPorts = input[0:pathSeparatorIndex]
	}
	commaSeparatorIndex := strings.Index(input, ":")
	if commaSeparatorIndex < 0 {
		err = fmt.Errorf("port should be specified in %s", input)
		return
	}
	address = ServerAddress(hostAndPorts)
	return
}
