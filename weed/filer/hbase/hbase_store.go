package hbase

import (
	"bytes"
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
	"io"
)

func init() {
	filer.Stores = append(filer.Stores, &HbaseStore{})
}

type HbaseStore struct {
	Client    gohbase.Client
	table     []byte
	cfKv      string
	cfMetaDir string
	column    string
}

func (store *HbaseStore) GetName() string {
	return "hbase"
}

func (store *HbaseStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetString(prefix+"zkquorum"),
		configuration.GetString(prefix+"table"),
	)
}

func (store *HbaseStore) initialize(zkquorum, table string) (err error) {
	store.Client = gohbase.NewClient(zkquorum)
	store.table = []byte(table)
	store.cfKv = "kv"
	store.cfMetaDir = "meta"
	store.column = "a"

	// check table exists
	key := "whatever"
	headers := map[string][]string{store.cfMetaDir: nil}
	get, err := hrpc.NewGet(context.Background(), store.table, []byte(key), hrpc.Families(headers))
	if err != nil {
		return fmt.Errorf("NewGet returned an error: %v", err)
	}
	_, err = store.Client.Get(get)
	if err != gohbase.TableNotFound {
		return nil
	}

	// create table
	adminClient := gohbase.NewAdminClient(zkquorum)
	cFamilies := []string{store.cfKv, store.cfMetaDir}
	cf := make(map[string]map[string]string, len(cFamilies))
	for _, f := range cFamilies {
		cf[f] = nil
	}
	ct := hrpc.NewCreateTable(context.Background(), []byte(table), cf)
	if err := adminClient.CreateTable(ct); err != nil {
		return err
	}

	return nil
}

func (store *HbaseStore) InsertEntry(ctx context.Context, entry *filer.Entry) error {
	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}
	if len(entry.Chunks) > 50 {
		value = util.MaybeGzipData(value)
	}

	return store.doPut(ctx, store.cfMetaDir, []byte(entry.FullPath), value)
}

func (store *HbaseStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.InsertEntry(ctx, entry)
}

func (store *HbaseStore) FindEntry(ctx context.Context, path util.FullPath) (entry *filer.Entry, err error) {
	value, err := store.doGet(ctx, store.cfMetaDir, []byte(path))
	if err != nil {
		if err == filer.ErrKvNotFound {
			return nil, filer_pb.ErrNotFound
		}
		return nil, err
	}

	entry = &filer.Entry{
		FullPath: path,
	}
	err = entry.DecodeAttributesAndChunks(util.MaybeDecompressData(value))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}
	return entry, nil
}

func (store *HbaseStore) DeleteEntry(ctx context.Context, path util.FullPath) (err error) {
	return store.doDelete(ctx, store.cfMetaDir, []byte(path))
}

func (store *HbaseStore) DeleteFolderChildren(ctx context.Context, path util.FullPath) (err error) {

	family := map[string][]string{store.cfMetaDir: {COLUMN_NAME}}
	expectedPrefix := []byte(path + "/")
	scan, err := hrpc.NewScanRange(ctx, store.table, expectedPrefix, nil, hrpc.Families(family))
	if err != nil {
		return err
	}

	scanner := store.Client.Scan(scan)
	defer scanner.Close()
	for {
		res, err := scanner.Next()
		if err != nil {
			break
		}
		if len(res.Cells) == 0 {
			continue
		}
		cell := res.Cells[0]

		if !bytes.HasPrefix(cell.Row, expectedPrefix) {
			break
		}
		fullpath := util.FullPath(cell.Row)
		dir, _ := fullpath.DirAndName()
		if dir != string(dirPath) {
			continue
		}

		err = store.doDelete(ctx, store.cfMetaDir, cell.Row)
		if err != nil {
			break
		}

	}
	return
}

func (store *HbaseStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int) ([]*filer.Entry, error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "")
}

func (store *HbaseStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int, prefix string) ([]*filer.Entry, error) {
	family := map[string][]string{store.cfMetaDir: {COLUMN_NAME}}
	expectedPrefix := []byte(string(dirPath) + "/" + prefix)
	scan, err := hrpc.NewScanRange(ctx, store.table, expectedPrefix, nil, hrpc.Families(family))
	if err != nil {
		return nil, err
	}

	var entries []*filer.Entry
	scanner := store.Client.Scan(scan)
	defer scanner.Close()
	for {
		res, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return entries, err
		}
		if len(res.Cells) == 0 {
			continue
		}
		cell := res.Cells[0]

		if !bytes.HasPrefix(cell.Row, expectedPrefix) {
			break
		}

		fullpath := util.FullPath(cell.Row)
		dir, fileName := fullpath.DirAndName()
		if dir != string(dirPath) {
			continue
		}

		value := cell.Value

		if fileName == startFileName && !includeStartFile {
			continue
		}

		limit--
		if limit < 0 {
			break
		}
		entry := &filer.Entry{
			FullPath: fullpath,
		}
		if decodeErr := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(value)); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (store *HbaseStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (store *HbaseStore) CommitTransaction(ctx context.Context) error {
	return nil
}

func (store *HbaseStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *HbaseStore) Shutdown() {
	store.Client.Close()
}
