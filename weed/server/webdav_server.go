package weed_server

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"golang.org/x/net/webdav"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/spf13/viper"
)

type WebDavOption struct {
	Filer            string
	FilerGrpcAddress string
	DomainName       string
	BucketsPath      string
	GrpcDialOption   grpc.DialOption
}

type WebDavServer struct {
	option         *WebDavOption
	secret         security.SigningKey
	filer          *filer2.Filer
	grpcDialOption grpc.DialOption
	Handler        *webdav.Handler
}

func NewWebDavServer(option *WebDavOption) (ws *WebDavServer, err error) {

	fs, _ := NewWebDavFileSystem()

	ws = &WebDavServer{
		option:         option,
		grpcDialOption: security.LoadClientTLS(viper.Sub("grpc"), "filer"),
		Handler: &webdav.Handler{
			FileSystem: fs,
			LockSystem: webdav.NewMemLS(),
			Logger: func(r *http.Request, err error) {
				litmus := r.Header.Get("X-Litmus")
				if len(litmus) > 19 {
					litmus = litmus[:16] + "..."
				}

				switch r.Method {
				case "COPY", "MOVE":
					dst := ""
					if u, err := url.Parse(r.Header.Get("Destination")); err == nil {
						dst = u.Path
					}
					glog.Infof("%-18s %s %s %v",
						r.Method,
						r.URL.Path,
						dst,
						err)
				default:
					glog.Infof("%-18s %s %v",
						r.Method,
						r.URL.Path,
						err)
				}
			},
		},
	}

	return ws, nil
}

// adapted from https://github.com/mattn/davfs/blob/master/plugin/mysql/mysql.go

type WebDavFileSystem struct {
}

type FileInfo struct {
	name     string
	size     int64
	mode     os.FileMode
	mod_time time.Time
}

func (fi *FileInfo) Name() string       { return fi.name }
func (fi *FileInfo) Size() int64        { return fi.size }
func (fi *FileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *FileInfo) ModTime() time.Time { return fi.mod_time }
func (fi *FileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi *FileInfo) Sys() interface{}   { return nil }

type WebDavFile struct {
	fs          *WebDavFileSystem
	name        string
	isDirectory bool
	off         int64
}

func NewWebDavFileSystem() (webdav.FileSystem, error) {
	return &WebDavFileSystem{}, nil
}

func clearName(name string) (string, error) {
	slashed := strings.HasSuffix(name, "/")
	name = path.Clean(name)
	if !strings.HasSuffix(name, "/") && slashed {
		name += "/"
	}
	if !strings.HasPrefix(name, "/") {
		return "", os.ErrInvalid
	}
	return name, nil
}

func (fs *WebDavFileSystem) Mkdir(ctx context.Context, name string, perm os.FileMode) error {

	glog.V(2).Infof("WebDavFileSystem.Mkdir %v", name)

	if !strings.HasSuffix(name, "/") {
		name += "/"
	}

	var err error
	if name, err = clearName(name); err != nil {
		return err
	}

	_, err = fs.stat(name)
	if err == nil {
		return os.ErrExist
	}

	base := "/"
	for _, elem := range strings.Split(strings.Trim(name, "/"), "/") {
		base += elem + "/"
		_, err = fs.stat(base)
		if err != os.ErrNotExist {
			return err
		}
		// _, err = fs.db.Exec(`insert into filesystem(name, content, mode, mod_time) values(?, '', ?, now())`, base, perm.Perm()|os.ModeDir)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fs *WebDavFileSystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {

	glog.V(2).Infof("WebDavFileSystem.OpenFile %v", name)

	var err error
	if name, err = clearName(name); err != nil {
		return nil, err
	}

	if flag&os.O_CREATE != 0 {
		// file should not have / suffix.
		if strings.HasSuffix(name, "/") {
			return nil, os.ErrInvalid
		}
		// based directory should be exists.
		dir, _ := path.Split(name)
		_, err := fs.stat(dir)
		if err != nil {
			return nil, os.ErrInvalid
		}
		_, err = fs.stat(name)
		if err == nil {
			if flag&os.O_EXCL != 0 {
				return nil, os.ErrExist
			}
			fs.removeAll(name)
		}
		// _, err = fs.db.Exec(`insert into filesystem(name, content, mode, mod_time) values(?, '', ?, now())`, name, perm.Perm())
		if err != nil {
			return nil, err
		}
		return &WebDavFile{fs, name, false, 0}, nil
	}

	fi, err := fs.stat(name)
	if err != nil {
		return nil, os.ErrNotExist
	}
	if !strings.HasSuffix(name, "/") && fi.IsDir() {
		name += "/"
	}

	return &WebDavFile{fs, name, true, 0}, nil

}

func (fs *WebDavFileSystem) removeAll(name string) error {
	var err error
	if name, err = clearName(name); err != nil {
		return err
	}

	fi, err := fs.stat(name)
	if err != nil {
		return err
	}

	if fi.IsDir() {
		//_, err = fs.db.Exec(`delete from filesystem where name like $1 escape '\'`, strings.Replace(name, `%`, `\%`, -1)+`%`)
	} else {
		//_, err = fs.db.Exec(`delete from filesystem where name = ?`, name)
	}
	return err
}

func (fs *WebDavFileSystem) RemoveAll(ctx context.Context, name string) error {

	glog.V(2).Infof("WebDavFileSystem.RemoveAll %v", name)

	return fs.removeAll(name)
}

func (fs *WebDavFileSystem) Rename(ctx context.Context, oldName, newName string) error {

	glog.V(2).Infof("WebDavFileSystem.Rename %v to %v", oldName, newName)

	var err error
	if oldName, err = clearName(oldName); err != nil {
		return err
	}
	if newName, err = clearName(newName); err != nil {
		return err
	}

	of, err := fs.stat(oldName)
	if err != nil {
		return os.ErrExist
	}
	if of.IsDir() && !strings.HasSuffix(oldName, "/") {
		oldName += "/"
		newName += "/"
	}

	_, err = fs.stat(newName)
	if err == nil {
		return os.ErrExist
	}

	//_, err = fs.db.Exec(`update filesystem set name = ? where name = ?`, newName, oldName)
	return err
}

func (fs *WebDavFileSystem) stat(name string) (os.FileInfo, error) {
	var err error
	if name, err = clearName(name); err != nil {
		return nil, err
	}

	//rows, err := fs.db.Query(`select name, format(length(content)/2, 0), mode, mod_time from filesystem where name = ?`, name)
	if err != nil {
		return nil, err
	}
	var fi FileInfo
	// err = rows.Scan(&fi.name, &fi.size, &fi.mode, &fi.mod_time)
	if err != nil {
		return nil, err
	}
	_, fi.name = path.Split(path.Clean(fi.name))
	if fi.name == "" {
		fi.name = "/"
		fi.mod_time = time.Now()
	}
	return &fi, nil
}

func (fs *WebDavFileSystem) Stat(ctx context.Context, name string) (os.FileInfo, error) {

	glog.V(2).Infof("WebDavFileSystem.Stat %v", name)

	return fs.stat(name)
}

func (f *WebDavFile) Write(p []byte) (int, error) {

	glog.V(2).Infof("WebDavFileSystem.Write %v", f.name)

	var err error
	// _, err := f.fs.db.Exec(`update filesystem set content = substr(content, 1, ?) || ? where name = ?`, f.off*2, hex.EncodeToString(p), f.name)
	if err != nil {
		return 0, err
	}
	//f.off += int64(len(p))
	return len(p), err
}

func (f *WebDavFile) Close() error {

	glog.V(2).Infof("WebDavFileSystem.Close %v", f.name)

	return nil
}

func (f *WebDavFile) Read(p []byte) (int, error) {

	glog.V(2).Infof("WebDavFileSystem.Read %v", f.name)

	var err error
	//rows, err := f.fs.db.Query(`select mode, substr(content, ?, ?) from filesystem where name = ?`, 1+f.off*2, len(p)*2, f.name)
	if err != nil {
		return 0, err
	}
	//defer rows.Close()

	return 0, io.EOF
}

func (f *WebDavFile) Readdir(count int) ([]os.FileInfo, error) {

	glog.V(2).Infof("WebDavFileSystem.Readdir %v", f.name)

	// return f.children[old:f.off], nil
	return nil, nil
}

func (f *WebDavFile) Seek(offset int64, whence int) (int64, error) {

	glog.V(2).Infof("WebDavFile.Seek %v %v %v", f.name, offset, whence)

	var err error
	switch whence {
	case 0:
		f.off = 0
	case 2:
		if fi, err := f.fs.stat(f.name); err != nil {
			return 0, err
		} else {
			f.off = fi.Size()
		}
	}
	f.off += offset
	return f.off, err
}

func (f *WebDavFile) Stat() (os.FileInfo, error) {

	glog.V(2).Infof("WebDavFile.Stat %v", f.name)

	return f.fs.stat(f.name)
}
