package util

import (
	"net"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

func DetectedHostAddress() string {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		glog.V(0).Infof("failed to detect net interfaces: %v", err)
		return ""
	}

	if v4Address := selectIpV4(netInterfaces); v4Address != "" {
		return v4Address
	}

	return "localhost"
}

func selectIpV4(netInterfaces []net.Interface) string {
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
					if ipNet.IP.To4() != nil && ipNet.IP.To16() == nil || ipNet.IP.To16() != nil && ipNet.IP.To4() == nil{
						return ipNet.IP.String()
					}
			}
		}
	}
	return ""
}

func JoinHostPort(host string, port int) string {
	portStr := strconv.Itoa(port)
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		return host + ":" + portStr
	}
	return net.JoinHostPort(host, portStr)
}
