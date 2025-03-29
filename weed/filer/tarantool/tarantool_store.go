//go:build tarantool
// +build tarantool

package tarantool

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	weed_util "github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/crud"
	"github.com/tarantool/go-tarantool/v2/pool"
)

const (
	tarantoolSpaceName = "filer_metadata"
)

func init() {
	filer.Stores = append(filer.Stores, &TarantoolStore{})
}

type TarantoolStore struct {
	pool *pool.ConnectionPool
}

func (store *TarantoolStore) GetName() string {
	return "tarantool"
}

func (store *TarantoolStore) Initialize(configuration weed_util.Configuration, prefix string) error {

	configuration.SetDefault(prefix+"address", "localhost:3301")
	configuration.SetDefault(prefix+"user", "guest")
	configuration.SetDefault(prefix+"password", "")
	configuration.SetDefault(prefix+"timeout", "5s")
	configuration.SetDefault(prefix+"maxReconnects", "1000")

	address := configuration.GetString(prefix + "address")
	user := configuration.GetString(prefix + "user")
	password := configuration.GetString(prefix + "password")

	timeoutStr := configuration.GetString(prefix + "timeout")
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return fmt.Errorf("parse tarantool store timeout: %v", err)
	}

	maxReconnects := configuration.GetInt(prefix + "maxReconnects")
	if maxReconnects < 0 {
		return fmt.Errorf("maxReconnects is negative")
	}

	addresses := strings.Split(address, ",")

	return store.initialize(addresses, user, password, timeout, uint(maxReconnects))
}

func (store *TarantoolStore) initialize(addresses []string, user string, password string, timeout time.Duration, maxReconnects uint) error {

	opts := tarantool.Opts{
		Timeout:       timeout,
		Reconnect:     time.Second,
		MaxReconnects: maxReconnects,
	}

	poolInstances := makePoolInstances(addresses, user, password, opts)
	poolOpts := pool.Opts{
		CheckTimeout: time.Second,
	}

	ctx := context.Background()
	p, err := pool.ConnectWithOpts(ctx, poolInstances, poolOpts)
	if err != nil {
		return fmt.Errorf("Can't create connection pool: %v", err)
	}

	_, err = p.Do(tarantool.NewPingRequest(), pool.ANY).Get()
	if err != nil {
		return err
	}

	store.pool = p

	return nil
}

func makePoolInstances(addresses []string, user string, password string, opts tarantool.Opts) []pool.Instance {
	poolInstances := make([]pool.Instance, 0, len(addresses))
	for i, address := range addresses {
		poolInstances = append(poolInstances, makePoolInstance(address, user, password, opts, i))
	}
	return poolInstances
}

func makePoolInstance(address string, user string, password string, opts tarantool.Opts, serial int) pool.Instance {
	return pool.Instance{
		Name: fmt.Sprintf("instance%d", serial),
		Dialer: tarantool.NetDialer{
			Address:  address,
			User:     user,
			Password: password,
		},
		Opts: opts,
	}
}

func (store *TarantoolStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (store *TarantoolStore) CommitTransaction(ctx context.Context) error {
	return nil
}

func (store *TarantoolStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *TarantoolStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		meta = util.MaybeGzipData(meta)
	}

	var ttl int64
	if entry.TtlSec > 0 {
		ttl = time.Now().Unix() + int64(entry.TtlSec)
	} else {
		ttl = 0
	}

	var operations = []crud.Operation{
		{
			Operator: crud.Insert,
			Field:    "data",
			Value:    string(meta),
		},
	}

	req := crud.MakeUpsertRequest(tarantoolSpaceName).
		Tuple([]interface{}{dir, nil, name, ttl, string(meta)}).
		Operations(operations)

	ret := crud.Result{}

	if err := store.pool.Do(req, pool.RW).GetTyped(&ret); err != nil {
		return fmt.Errorf("insert %s: %s", entry.FullPath, err)
	}

	return nil
}

func (store *TarantoolStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.InsertEntry(ctx, entry)
}

func (store *TarantoolStore) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer.Entry, err error) {
	dir, name := fullpath.DirAndName()

	findEntryGetOpts := crud.GetOpts{
		Fields:        crud.MakeOptTuple([]interface{}{"data"}),
		Mode:          crud.MakeOptString("read"),
		PreferReplica: crud.MakeOptBool(true),
		Balance:       crud.MakeOptBool(true),
	}

	req := crud.MakeGetRequest(tarantoolSpaceName).
		Key(crud.Tuple([]interface{}{dir, name})).
		Opts(findEntryGetOpts)

	resp := crud.Result{}

	err = store.pool.Do(req, pool.PreferRO).GetTyped(&resp)
	if err != nil {
		return nil, err
	}

	results, ok := resp.Rows.([]interface{})
	if !ok || len(results) != 1 {
		return nil, filer_pb.ErrNotFound
	}

	rows, ok := results[0].([]interface{})
	if !ok || len(rows) != 1 {
		return nil, filer_pb.ErrNotFound
	}

	row, ok := rows[0].(string)
	if !ok {
		return nil, fmt.Errorf("Can't convert rows[0] field to string. Actual type: %v, value: %v", reflect.TypeOf(rows[0]), rows[0])
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}

	err = entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData([]byte(row)))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *TarantoolStore) DeleteEntry(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	dir, name := fullpath.DirAndName()

	delOpts := crud.DeleteOpts{
		Noreturn: crud.MakeOptBool(true),
	}

	req := crud.MakeDeleteRequest(tarantoolSpaceName).
		Key(crud.Tuple([]interface{}{dir, name})).
		Opts(delOpts)

	if _, err := store.pool.Do(req, pool.RW).Get(); err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *TarantoolStore) DeleteFolderChildren(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	req := tarantool.NewCallRequest("filer_metadata.delete_by_directory_idx").
		Args([]interface{}{fullpath})

	if _, err := store.pool.Do(req, pool.RW).Get(); err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *TarantoolStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return lastFileName, filer.ErrUnsupportedListDirectoryPrefixed
}

func (store *TarantoolStore) ListDirectoryEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {

	req := tarantool.NewCallRequest("filer_metadata.find_by_directory_idx_and_name").
		Args([]interface{}{string(dirPath), startFileName, includeStartFile, limit})

	results, err := store.pool.Do(req, pool.PreferRO).Get()
	if err != nil {
		return
	}

	if len(results) < 1 {
		glog.Errorf("Can't find results, data is empty")
		return
	}

	rows, ok := results[0].([]interface{})
	if !ok {
		glog.Errorf("Can't convert results[0] to list")
		return
	}

	for _, result := range rows {
		row, ok := result.([]interface{})
		if !ok {
			glog.Errorf("Can't convert result to list")
			return
		}

		if len(row) < 5 {
			glog.Errorf("Length of result is less than needed: %v", len(row))
			return
		}

		nameRaw := row[2]
		name, ok := nameRaw.(string)
		if !ok {
			glog.Errorf("Can't convert name field to string. Actual type: %v, value: %v", reflect.TypeOf(nameRaw), nameRaw)
			return
		}

		dataRaw := row[4]
		data, ok := dataRaw.(string)
		if !ok {
			glog.Errorf("Can't convert data field to string. Actual type: %v, value: %v", reflect.TypeOf(dataRaw), dataRaw)
			return
		}

		entry := &filer.Entry{
			FullPath: util.NewFullPath(string(dirPath), name),
		}
		lastFileName = name
		if decodeErr := entry.DecodeAttributesAndChunks(util.MaybeDecompressData([]byte(data))); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}
		if !eachEntryFunc(entry) {
			break
		}
	}

	return lastFileName, err
}

func (store *TarantoolStore) Shutdown() {
	store.pool.Close()
}
