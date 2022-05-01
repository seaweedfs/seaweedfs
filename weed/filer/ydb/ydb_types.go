package ydb

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
