package util

import (
	"net"

	"github.com/chrislusf/seaweedfs/weed/util/log"
)

func DetectedHostAddress() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Infof("failed to detect ip address: %v", err)
		return ""
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}

	return "localhost"
}
