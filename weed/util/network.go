package util

import (
	"net"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

func DetectedHostAddress() string {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		glog.V(0).Infof("failed to detect net interfaces: %v", err)
		return ""
	}

	for _, netInterface := range netInterfaces {
		if (netInterface.Flags & net.FlagUp) == 0 {
			continue
		}
		addrs, err := netInterface.Addrs()
		if err != nil {
			glog.V(0).Infof("get interface addresses: %v", err)
		}

		for _, a := range addrs {
			if ipNet, ok := a.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
				if ipNet.IP.To4() != nil {
					return ipNet.IP.String()
				}
			}
		}
	}

	return "localhost"
}
