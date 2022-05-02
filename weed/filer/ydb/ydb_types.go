package ydb

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

//go:generate ydbgen

//ydb:gen
type FileMeta struct {
	DirHash   int64  `ydb:"type:int64"`
	Name      string `ydb:"type:utf8"`
	Directory string `ydb:"type:utf8"`
	Meta      []byte `ydb:"-"`
}

//ydb:gen scan,value
type FileMetas []FileMeta

func (fm *FileMeta) queryParameters() *table.QueryParameters {
	return table.NewQueryParameters(
		table.ValueParam("$dir_hash", types.Int64Value(fm.DirHash)),
		table.ValueParam("$name", types.UTF8Value(fm.Name)),
		table.ValueParam("$directory", types.UTF8Value(fm.Directory)),
		table.ValueParam("$meta", types.StringValue(fm.Meta)))
}

func createTableOptions() []options.CreateTableOption {
	return []options.CreateTableOption{
		options.WithColumn("dir_hash", types.TypeUint64),
		options.WithColumn("name", types.TypeUTF8),
		options.WithColumn("directory", types.TypeUTF8),
		options.WithColumn("meta", types.TypeString),
		options.WithPrimaryKeyColumn("dir_hash", "name"),
	}
}
func withPragma(prefix, query string) string {
	return `PRAGMA TablePathPrefix("` + prefix + `");` + query
}
