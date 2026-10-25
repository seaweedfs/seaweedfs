package api

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/seaweedfs/seaweedfs/telemetry/proto"
)

// The collect endpoint is anonymous, so reports can't be authenticated —
// these checks only reject values a real SeaweedFS master can't produce,
// to keep casual junk out of the collected data.

const (
	maxRequestBytes = 4096
	maxServerCount  = 100_000       // volume servers / filers / brokers per cluster
	maxVolumeCount  = 100_000_000   // volumes per cluster
	maxDiskBytes    = uint64(1) << 60 // 1 EiB
)

var (
	// TopologyId is a raft-generated UUID.
	uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	// Version is VERSION_NUMBER, e.g. "4.40", plus "-enterprise" for enterprise builds.
	versionRe = regexp.MustCompile(`^\d+\.\d+(-enterprise)?$`)

	validGoos = map[string]bool{
		"linux": true, "darwin": true, "windows": true,
		"freebsd": true, "openbsd": true, "netbsd": true,
	}
	validGoarch = map[string]bool{
		"amd64": true, "arm64": true, "arm": true, "386": true,
		"riscv64": true, "ppc64le": true, "s390x": true,
		"mips64": true, "mips64le": true, "loong64": true,
	}
)

func validateTelemetryData(data *proto.TelemetryData) error {
	if !uuidRe.MatchString(data.TopologyId) {
		return fmt.Errorf("topology_id is not a UUID")
	}
	if !versionRe.MatchString(data.Version) {
		return fmt.Errorf("unrecognized version")
	}
	goos, goarch, ok := strings.Cut(data.Os, "/")
	if !ok || !validGoos[goos] || !validGoarch[goarch] {
		return fmt.Errorf("unrecognized os")
	}
	if data.VolumeServerCount < 0 || data.VolumeServerCount > maxServerCount ||
		data.FilerCount < 0 || data.FilerCount > maxServerCount ||
		data.BrokerCount < 0 || data.BrokerCount > maxServerCount ||
		data.TotalVolumeCount < 0 || data.TotalVolumeCount > maxVolumeCount ||
		data.TotalDiskBytes > maxDiskBytes {
		return fmt.Errorf("values out of range")
	}
	return nil
}
