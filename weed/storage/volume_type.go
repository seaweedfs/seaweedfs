package storage

import "fmt"

type VolumeType string

const (
	HardDriveType VolumeType = ""
	SsdType                  = "ssd"
)

func ToVolumeType(vt string) (volumeType VolumeType, err error) {
	volumeType = HardDriveType
	switch vt {
	case "", "hdd":
		volumeType = HardDriveType
	case "ssd":
		volumeType = SsdType
	default:
		err = fmt.Errorf("parse VolumeType %s: expecting hdd or ssd\n", vt)
	}
	return
}
