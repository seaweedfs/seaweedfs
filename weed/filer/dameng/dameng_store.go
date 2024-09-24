package dameng

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"strings"
	"time"

	_ "gitee.com/chunanyong/dm"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	CONNECTION_URL_PATTERN = "dm://%s:%s@%s:%d?schema=%s"
)

func init() {
	filer.Stores = append(filer.Stores, &DamengStore{})
}

type DamengStore struct {
	abstract_sql.AbstractSqlStore
}

func (store *DamengStore) GetName() string {
	return "dameng"
}

func (store *DamengStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetString(prefix+"dsn"),
		configuration.GetString(prefix+"upsertQuery"),
		configuration.GetBool(prefix+"enableUpsert"),
		configuration.GetString(prefix+"username"),
		configuration.GetString(prefix+"password"),
		configuration.GetString(prefix+"hostname"),
		configuration.GetInt(prefix+"port"),
		configuration.GetString(prefix+"database"),
		configuration.GetInt(prefix+"connection_max_idle"),
		configuration.GetInt(prefix+"connection_max_open"),
		configuration.GetInt(prefix+"connection_max_lifetime_seconds"),
		configuration.GetBool(prefix+"interpolateParams"),
	)
}

func (store *DamengStore) initialize(dsn string, upsertQuery string, enableUpsert bool, user, password, hostname string, port int, database string, maxIdle, maxOpen,
	maxLifetimeSeconds int, interpolateParams bool) (err error) {

	store.SupportBucketTable = false
	if !enableUpsert {
		upsertQuery = ""
	}
	store.SqlGenerator = &SqlGenDameng{
		CreateTableSqlTemplate: "",
		DropTableSqlTemplate:   "DROP TABLE `%s`",
		UpsertQueryTemplate:    upsertQuery,
	}

	dsn = fmt.Sprintf(CONNECTION_URL_PATTERN, user, password, hostname, port, database)

	var dbErr error
	store.DB, dbErr = sql.Open("dm", dsn)
	if dbErr != nil {
		store.DB.Close()
		store.DB = nil
		return fmt.Errorf("can not connect to %s error:%v", strings.ReplaceAll(dsn, "", "<ADAPTED>"), err)
	}

	store.DB.SetMaxIdleConns(maxIdle)
	store.DB.SetMaxOpenConns(maxOpen)
	store.DB.SetConnMaxLifetime(time.Duration(maxLifetimeSeconds) * time.Second)

	if err = store.DB.Ping(); err != nil {
		return fmt.Errorf("connect to %s error:%v", strings.ReplaceAll(dsn, "", "<ADAPTED>"), err)
	}

	return nil
}

func (store *DamengStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (store *DamengStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	db, bucket, shortPath, err := store.GetTxOrDB(ctx, dirPath, true)
	if err != nil {
		return lastFileName, fmt.Errorf("findDB %s : %v", dirPath, err)
	}

	sqlText := store.GetSqlListExclusive(bucket)
	if includeStartFile {
		sqlText = store.GetSqlListInclusive(bucket)
	}

	rows, err := db.QueryContext(ctx, sqlText, util.HashStringToLong(string(shortPath)), string(shortPath), startFileName, string(shortPath), limit)
	if err != nil {
		return lastFileName, fmt.Errorf("list %s : %v", dirPath, err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var data []byte
		if err = rows.Scan(&name, &data); err != nil {
			glog.V(0).Infof("scan %s : %v", dirPath, err)
			return lastFileName, fmt.Errorf("scan %s: %v", dirPath, err)
		}
		lastFileName = name

		entry := &filer.Entry{
			FullPath: util.NewFullPath(string(dirPath), name),
		}
		if err = entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data)); err != nil {
			glog.V(0).Infof("scan decode %s : %v", entry.FullPath, err)
			return lastFileName, fmt.Errorf("scan decode %s : %v", entry.FullPath, err)
		}

		if !eachEntryFunc(entry) {
			break
		}

	}

	return lastFileName, nil
}
