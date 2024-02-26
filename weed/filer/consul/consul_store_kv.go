package consul

import (
	"context"
	"fmt"

	consul "github.com/hashicorp/consul/api"
	"github.com/seaweedfs/seaweedfs/weed/filer"
)

func (store *ConsulStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	data := &consul.KVPair{Key: store.key(string(key)), Value: value}
	_, err = store.client.KV().Put(data, &consul.WriteOptions{})

	if err != nil {
		return fmt.Errorf("kv put: %v", err)
	}

	return nil
}

func (store *ConsulStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	resp, _, err := store.client.KV().Get(store.key(string(key)), &consul.QueryOptions{})

	if err != nil {
		return nil, fmt.Errorf("kv get: %v", err)
	}

	if resp == nil {
		return nil, filer.ErrKvNotFound
	}

	return resp.Value, nil
}

func (store *ConsulStore) KvDelete(ctx context.Context, key []byte) (err error) {
	_, err = store.client.KV().Delete(store.key(string(key)), &consul.WriteOptions{})

	if err != nil {
		return fmt.Errorf("kv delete: %v", err)
	}

	return nil
}
