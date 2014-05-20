package storage

import (
	"code.google.com/p/weed-fs/go/glog"
	"os"
)

func (v *Volume) garbageLevel() float64 {
	return float64(v.nm.DeletedSize()) / float64(v.ContentSize())
}

func (v *Volume) Compact() error {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()

	filePath := v.FileName()
	glog.V(3).Infof("creating copies for volume %d ...", v.Id)
	return v.copyDataAndGenerateIndexFile(filePath+".cpd", filePath+".cpx")
}
func (v *Volume) commitCompact() error {
	v.accessLock.Lock()
	defer v.accessLock.Unlock()
	_ = v.dataFile.Close()
	var e error
	if e = os.Rename(v.FileName()+".cpd", v.FileName()+".dat"); e != nil {
		return e
	}
	if e = os.Rename(v.FileName()+".cpx", v.FileName()+".idx"); e != nil {
		return e
	}
	if e = v.load(true, false); e != nil {
		return e
	}
	return nil
}
