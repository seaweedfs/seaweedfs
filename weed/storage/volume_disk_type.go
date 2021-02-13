package storage

import (
	"strings"
)

type DiskType string

const (
	HardDriveType DiskType = ""
	SsdType                = "ssd"
)

func ToDiskType(vt string) (diskType DiskType) {
	vt = strings.ToLower(vt)
	diskType = HardDriveType
	switch vt {
	case "", "hdd":
		diskType = HardDriveType
	case "ssd":
		diskType = SsdType
	default:
		diskType = DiskType(vt)
	}
	return
}
