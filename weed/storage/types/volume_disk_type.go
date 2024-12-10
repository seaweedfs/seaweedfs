package types

import (
	"strings"
)

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
