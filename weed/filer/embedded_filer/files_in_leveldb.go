package embedded_filer

import (
	"bytes"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

/*
The entry in level db has this format:
  key: genKey(dirId, fileName)
  value: []byte(fid)
And genKey(dirId, fileName) use first 4 bytes to store dirId, and rest for fileName
*/

type FileListInLevelDb struct {
	db *leveldb.DB
}

func NewFileListInLevelDb(dir string) (fl *FileListInLevelDb, err error) {
	fl = &FileListInLevelDb{}
	if fl.db, err = leveldb.OpenFile(dir, nil); err != nil {
		return
	}
	return
}

func genKey(dirId filer.DirectoryId, fileName string) []byte {
	ret := make([]byte, 0, 4+len(fileName))
	for i := 3; i >= 0; i-- {
		ret = append(ret, byte(dirId>>(uint(i)*8)))
	}
	ret = append(ret, []byte(fileName)...)
	return ret
}

func (fl *FileListInLevelDb) CreateFile(dirId filer.DirectoryId, fileName string, fid string) (err error) {
	glog.V(4).Infoln("directory", dirId, "fileName", fileName, "fid", fid)
	return fl.db.Put(genKey(dirId, fileName), []byte(fid), nil)
}
func (fl *FileListInLevelDb) DeleteFile(dirId filer.DirectoryId, fileName string) (fid string, err error) {
	if fid, err = fl.FindFile(dirId, fileName); err != nil {
		if err == leveldb.ErrNotFound {
			return "", nil
		}
		return
	}
	err = fl.db.Delete(genKey(dirId, fileName), nil)
	return fid, err
}
func (fl *FileListInLevelDb) FindFile(dirId filer.DirectoryId, fileName string) (fid string, err error) {
	data, e := fl.db.Get(genKey(dirId, fileName), nil)
	if e != nil {
		return "", e
	}
	return string(data), nil
}
func (fl *FileListInLevelDb) ListFiles(dirId filer.DirectoryId, lastFileName string, limit int) (files []filer.FileEntry) {
	glog.V(4).Infoln("directory", dirId, "lastFileName", lastFileName, "limit", limit)
	dirKey := genKey(dirId, "")
	iter := fl.db.NewIterator(&util.Range{Start: genKey(dirId, lastFileName)}, nil)
	limitCounter := 0
	for iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, dirKey) {
			break
		}
		fileName := string(key[len(dirKey):])
		if fileName == lastFileName {
			continue
		}
		limitCounter++
		if limit > 0 {
			if limitCounter > limit {
				break
			}
		}
		files = append(files, filer.FileEntry{Name: fileName, Id: filer.FileId(string(iter.Value()))})
	}
	iter.Release()
	return
}
