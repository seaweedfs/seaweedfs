// +build ignore

package metastore

import (
	"code.google.com/p/weed-fs/go/glog"
	"errors"
	"github.com/coreos/go-etcd/etcd"
	"strings"
)

// store data on etcd

type MetaStoreEtcdBacking struct {
	client *etcd.Client
}

func NewMetaStoreEtcdBacking(etcdCluster string) *MetaStoreEtcdBacking {
	m := &MetaStoreEtcdBacking{}
	m.client = etcd.NewClient(strings.Split(etcdCluster, ","))
	return m
}

func (m MetaStoreEtcdBacking) Set(path, val string) error {
	res, e := m.client.Set(path, val, 0)
	glog.V(2).Infof("etcd set response: %+v\n", res)
	return e
}

func (m MetaStoreEtcdBacking) Get(path string) (string, error) {
	results, err := m.client.Get(path)
	for i, res := range results {
		glog.V(2).Infof("[%d] get response: %+v\n", i, res)
	}
	if err != nil {
		return "", err
	}
	if results[0].Key != path {
		return "", errors.New("Key Not Found:" + path)
	}
	return results[0].Value, nil
}

func (m MetaStoreEtcdBacking) Has(path string) (ok bool) {
	results, err := m.client.Get(path)
	if err != nil {
		return false
	}
	if results[0].Key != path {
		return false
	}
	return true
}
