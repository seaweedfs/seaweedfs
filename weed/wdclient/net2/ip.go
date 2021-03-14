package net2

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

var myHostname string
var myHostnameOnce sync.Once

// Like os.Hostname but caches first successful result, making it cheap to call it
// over and over.
// It will also crash whole process if fetching Hostname fails!
func MyHostname() string {
	myHostnameOnce.Do(func() {
		var err error
		myHostname, err = os.Hostname()
		if err != nil {
			log.Fatal(err)
		}
	})
	return myHostname
}

var myIp4 *net.IPAddr
var myIp4Once sync.Once

// Resolves `MyHostname()` to an Ip4 address. Caches first successful result, making it
// cheap to call it over and over.
// It will also crash whole process if resolving the IP fails!
func MyIp4() *net.IPAddr {
	myIp4Once.Do(func() {
		var err error
		myIp4, err = net.ResolveIPAddr("ip4", MyHostname())
		if err != nil {
			log.Fatal(err)
		}
	})
	return myIp4
}

var myIp6 *net.IPAddr
var myIp6Once sync.Once

// Resolves `MyHostname()` to an Ip6 address. Caches first successful result, making it
// cheap to call it over and over.
// It will also crash whole process if resolving the IP fails!
func MyIp6() *net.IPAddr {
	myIp6Once.Do(func() {
		var err error
		myIp6, err = net.ResolveIPAddr("ip6", MyHostname())
		if err != nil {
			log.Fatal(err)
		}
	})
	return myIp6
}

// This returns the list of local ip addresses which other hosts can connect
// to (NOTE: Loopback ip is ignored).
// Also resolves Hostname to an address and adds it to the list too, so
// IPs from /etc/hosts can work too.
func GetLocalIPs() ([]*net.IP, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("Failed to lookup hostname: %v", err)
	}
	// Resolves IP Address from Hostname, this way overrides in /etc/hosts
	// can work too for IP resolution.
	ipInfo, err := net.ResolveIPAddr("ip4", hostname)
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve ip: %v", err)
	}
	ips := []*net.IP{&ipInfo.IP}

	// TODO(zviad): Is rest of the code really necessary?
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("Failed to get interface addresses: %v", err)
	}
	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}

		if ipnet.IP.IsLoopback() {
			continue
		}

		ips = append(ips, &ipnet.IP)
	}
	return ips, nil
}

var localhostIPNets []*net.IPNet

func init() {
	for _, mask := range []string{"127.0.0.1/8", "::1/128"} {
		_, ipnet, err := net.ParseCIDR(mask)
		if err != nil {
			panic(err)
		}
		localhostIPNets = append(localhostIPNets, ipnet)
	}
}

func IsLocalhostIp(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	for _, ipnet := range localhostIPNets {
		if ipnet.Contains(ip) {
			return true
		}
	}
	return false
}

// Given a host string, return true if the host is an ip (v4/v6) localhost.
func IsLocalhost(host string) bool {
	return IsLocalhostIp(host) ||
		host == "localhost" ||
		host == "ip6-localhost" ||
		host == "ipv6-localhost"
}

// Resolves hostnames in addresses to actual IP4 addresses. Skips all invalid addresses
// and all addresses that can't be resolved.
// `addrs` are assumed to be of form: ["<hostname>:<port>", ...]
// Returns an error in addition to resolved addresses if not all resolutions succeed.
func ResolveIP4s(addrs []string) ([]string, error) {
	resolvedAddrs := make([]string, 0, len(addrs))
	var lastErr error

	for _, server := range addrs {
		hostPort := strings.Split(server, ":")
		if len(hostPort) != 2 {
			lastErr = fmt.Errorf("Skipping invalid address: %s", server)
			continue
		}

		ip, err := net.ResolveIPAddr("ip4", hostPort[0])
		if err != nil {
			lastErr = err
			continue
		}
		resolvedAddrs = append(resolvedAddrs, ip.IP.String()+":"+hostPort[1])
	}
	return resolvedAddrs, lastErr
}

func LookupValidAddrs() (map[string]bool, error) {
	hostName, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	addrs, err := net.LookupHost(hostName)
	if err != nil {
		return nil, err
	}
	validAddrs := make(map[string]bool)
	validAddrs[hostName] = true
	for _, addr := range addrs {
		validAddrs[addr] = true
	}
	// Special case localhost/127.0.0.1 so that this works on devVMs. It should
	// have no affect in production.
	validAddrs["127.0.0.1"] = true
	validAddrs["localhost"] = true
	return validAddrs, nil
}
