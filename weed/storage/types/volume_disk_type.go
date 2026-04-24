package types

import (
	"strings"
)

// DiskId identifies a single physical disk on a volume server, matching the
// uint32 index into Store.Locations that the volume server assigns per mount
// point. It is carried on the wire as uint32 in VolumeEcShardInformationMessage
// and VolumeInformationMessage.
type DiskId uint32

type DiskType string

const (
	HardDriveType DiskType = ""
	HddType                = "hdd"
	SsdType                = "ssd"
)

func ToDiskType(vt string) (diskType DiskType) {
	vt = strings.ToLower(vt)
	diskType = HardDriveType
	switch vt {
	case "", HddType:
		diskType = HardDriveType
	case "ssd":
		diskType = SsdType
	default:
		diskType = DiskType(vt)
	}
	return
}

func (diskType DiskType) String() string {
	if diskType == "" {
		return ""
	}
	return string(diskType)
}

func (diskType DiskType) ReadableString() string {
	if diskType == "" {
		return HddType
	}
	return string(diskType)
}
