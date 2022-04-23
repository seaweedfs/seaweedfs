//go:build hdfs
// +build hdfs

package hdfs

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/remote_pb"
	"github.com/chrislusf/seaweedfs/weed/remote_storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	hdfs "github.com/colinmarc/hdfs/v2"
	"io"
	"os"
	"path"
)

func init() {
	remote_storage.RemoteStorageClientMakers["hdfs"] = new(hdfsRemoteStorageMaker)
}

type hdfsRemoteStorageMaker struct{}

func (s hdfsRemoteStorageMaker) HasBucket() bool {
	return false
}

func (s hdfsRemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	client := &hdfsRemoteStorageClient{
		conf: conf,
	}

	options := hdfs.ClientOptions{
		Addresses:           conf.HdfsNamenodes,
		UseDatanodeHostname: false,
	}

	if conf.HdfsServicePrincipalName != "" {
		var err error
		options.KerberosClient, err = getKerberosClient()
		if err != nil {
			return nil, fmt.Errorf("get kerberos authentication: %s", err)
		}
		options.KerberosServicePrincipleName = conf.HdfsServicePrincipalName

		if conf.HdfsDataTransferProtection != "" {
			options.DataTransferProtection = conf.HdfsDataTransferProtection
		}
	} else {
		options.User = conf.HdfsUsername
	}

	c, err := hdfs.NewClient(options)
	if err != nil {
		return nil, err
	}

	client.client = c
	return client, nil
}

type hdfsRemoteStorageClient struct {
	conf   *remote_pb.RemoteConf
	client *hdfs.Client
}

var _ = remote_storage.RemoteStorageClient(&hdfsRemoteStorageClient{})

func (c *hdfsRemoteStorageClient) Traverse(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) (err error) {

	return remote_storage.TraverseBfs(func(parentDir util.FullPath, visitFn remote_storage.VisitFunc) error {
		children, err := c.client.ReadDir(string(parentDir))
		if err != nil {
			return err
		}
		for _, child := range children {
			if err := visitFn(string(parentDir), child.Name(), child.IsDir(), &filer_pb.RemoteEntry{
				StorageName:       c.conf.Name,
				LastLocalSyncTsNs: 0,
				RemoteETag:        "",
				RemoteMtime:       child.ModTime().Unix(),
				RemoteSize:        child.Size(),
			}); err != nil {
				return nil
			}
		}
		return nil
	}, util.FullPath(loc.Path), visitFn)

}
func (c *hdfsRemoteStorageClient) ReadFile(loc *remote_pb.RemoteStorageLocation, offset int64, size int64) (data []byte, err error) {

	f, err := c.client.Open(loc.Path)
	if err != nil {
		return
	}
	defer f.Close()
	data = make([]byte, size)
	_, err = f.ReadAt(data, offset)

	return

}

func (c *hdfsRemoteStorageClient) WriteDirectory(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry) (err error) {
	return c.client.MkdirAll(loc.Path, os.FileMode(entry.Attributes.FileMode))
}

func (c *hdfsRemoteStorageClient) RemoveDirectory(loc *remote_pb.RemoteStorageLocation) (err error) {
	return c.client.RemoveAll(loc.Path)
}

func (c *hdfsRemoteStorageClient) WriteFile(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry, reader io.Reader) (remoteEntry *filer_pb.RemoteEntry, err error) {

	dirname := path.Dir(loc.Path)

	// ensure parent directory
	if err = c.client.MkdirAll(dirname, 0755); err != nil {
		return
	}

	// remove existing file
	info, err := c.client.Stat(loc.Path)
	if err == nil {
		err = c.client.Remove(loc.Path)
		if err != nil {
			return
		}
	}

	// create new file
	out, err := c.client.Create(loc.Path)
	if err != nil {
		return
	}

	cleanup := func() {
		if removeErr := c.client.Remove(loc.Path); removeErr != nil {
			glog.Errorf("clean up %s%s: %v", loc.Name, loc.Path, removeErr)
		}
	}

	if _, err = io.Copy(out, reader); err != nil {
		cleanup()
		return
	}

	if err = out.Close(); err != nil {
		cleanup()
		return
	}

	info, err = c.client.Stat(loc.Path)
	if err != nil {
		return
	}

	return &filer_pb.RemoteEntry{
		RemoteMtime: info.ModTime().Unix(),
		RemoteSize:  info.Size(),
		RemoteETag:  "",
		StorageName: c.conf.Name,
	}, nil

}

func (c *hdfsRemoteStorageClient) UpdateFileMetadata(loc *remote_pb.RemoteStorageLocation, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) error {
	if oldEntry.Attributes.FileMode != newEntry.Attributes.FileMode {
		if err := c.client.Chmod(loc.Path, os.FileMode(newEntry.Attributes.FileMode)); err != nil {
			return err
		}
	}
	return nil
}

func (c *hdfsRemoteStorageClient) DeleteFile(loc *remote_pb.RemoteStorageLocation) (err error) {
	if err = c.client.Remove(loc.Path); err != nil {
		return fmt.Errorf("hdfs delete %s: %v", loc.Path, err)
	}
	return
}

func (c *hdfsRemoteStorageClient) ListBuckets() (buckets []*remote_storage.Bucket, err error) {
	return
}

func (c *hdfsRemoteStorageClient) CreateBucket(name string) (err error) {
	return
}

func (c *hdfsRemoteStorageClient) DeleteBucket(name string) (err error) {
	return
}
