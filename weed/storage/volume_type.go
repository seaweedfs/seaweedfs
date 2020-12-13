package storage

import "fmt"

type DiskType string

const (
	HardDriveType DiskType = ""
	SsdType                  = "ssd"
)

func ToDiskType(vt string) (diskType DiskType, err error) {
	diskType = HardDriveType
	switch vt {
	case "", "hdd":
		diskType = HardDriveType
	case "ssd":
		diskType = SsdType
	default:
		err = fmt.Errorf("parse DiskType %s: expecting hdd or ssd\n", vt)
	}
	return
}
