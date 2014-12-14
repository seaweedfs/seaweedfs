package stats

type DiskStatus struct {
	Dir  string
	All  uint64
	Used uint64
	Free uint64
}

func NewDiskStatus(path string) (disk *DiskStatus) {
	disk = &DiskStatus{Dir: path}
	disk.fillInStatus()
	return
}
