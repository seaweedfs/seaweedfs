package weed_server

import (
	"code.google.com/p/weed-fs/go/glog"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"net/http"
	"strings"
)

/*
1. level db is only for local instance
2. db stores two types of pairs
 <path/to/dir, sub folder names>
 <path/to/file, file id>
 So, to list a directory, just get the directory entry, and iterate the current directory files
 Care must be taken to maintain the <dir, sub dirs> and <file, fileid> pairs.
3.
*/
type FilerServer struct {
	port       string
	master     string
	collection string
	db         *leveldb.DB
}

func NewFilerServer(r *http.ServeMux, master string, dir string) (fs *FilerServer, err error) {
	fs = &FilerServer{
		master:     master,
		collection: "",
		port:       ":8888",
	}

	if fs.db, err = leveldb.OpenFile(dir, nil); err != nil {
		return
	}

	r.HandleFunc("/", fs.filerHandler)

	glog.V(0).Infoln("file server started on port ", fs.port)

	return fs, nil
}

func (fs *FilerServer) CreateFile(fullFileName string, fid string) (err error) {
	fs.ensureFileFolder(fullFileName)
	return fs.db.Put([]byte(fullFileName), []byte(fid), nil)
}

func (fs *FilerServer) FindFile(fullFileName string) (fid string, err error) {
	return fs.findEntry(fullFileName)
}

func (fs *FilerServer) ListDirectories(fullpath string) (dirs []string, err error) {
	data, e := fs.db.Get([]byte(fullpath), nil)
	if e != nil {
		return nil, e
	}
	return strings.Split(string(data), ":"), nil
}

func (fs *FilerServer) ListFiles(fullpath string, start, limit int) (files []string) {
	if !strings.HasSuffix(fullpath, "/") {
		fullpath += "/"
	}
	iter := fs.db.NewIterator(&util.Range{Start: []byte(fullpath)}, nil)
	startCounter, limitCounter := -1, 0
	for iter.Next() {
		startCounter++
		if startCounter < start {
			continue
		}
		limitCounter++
		if limit > 0 {
			if limitCounter > limit {
				break
			}
		}
		key := string(iter.Key())
		if !strings.HasPrefix(key, fullpath) {
			break
		}
		fileName := key[len(fullpath):]
		if strings.Contains(fileName, "/") {
			break
		}
		files = append(files, fileName)
	}
	iter.Release()
	return
}

func (fs *FilerServer) Delete(fullpath string, isForceDirectoryRemoval bool) (fid string, isFile bool, err error) {
	val, e := fs.findEntry(fullpath)
	if e != nil {
		return "", false, e
	}
	if strings.Contains(val, ",") {
		return val, true, fs.db.Delete([]byte(fullpath), nil)
	}
	// deal with directory
	if !strings.HasSuffix(fullpath, "/") {
		fullpath += "/"
	}
	iter := fs.db.NewIterator(&util.Range{Start: []byte(fullpath)}, nil)
	counter := 0
	for iter.Next() {
		counter++
		if counter > 0 {
			break
		}
	}
	iter.Release()
	if counter > 0 {
		return "", false, errors.New("Force Deletion Not Supported Yet")
	}
	return "", false, fs.db.Delete([]byte(fullpath), nil)
}

func (fs *FilerServer) findEntry(fullpath string) (value string, err error) {
	data, e := fs.db.Get([]byte(fullpath), nil)
	if e != nil {
		return "", e
	}
	return string(data), nil
}

func (fs *FilerServer) ensureFileFolder(fullFileName string) (err error) {
	parts := strings.Split(fullFileName, "/")
	path := "/"
	for i := 1; i < len(parts)-1; i++ {
		sub := parts[i]
		if sub == "" {
			continue
		}
		if err = fs.ensureFolderHasEntry(path, sub); err != nil {
			return
		}
		path = path + sub + "/"
	}
	return nil
}
func (fs *FilerServer) ensureFolderHasEntry(path string, sub string) (err error) {
	val, e := fs.findEntry(path)
	if e == leveldb.ErrNotFound {
		return fs.db.Put([]byte(path), []byte(sub), nil)
	} else if e != nil {
		return e
	}
	for _, v := range strings.Split(val, ":") {
		if v == sub {
			return nil
		}
	}
	return fs.db.Put([]byte(path), []byte(val+":"+sub), nil)
}
