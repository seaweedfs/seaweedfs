//go:build ydb
// +build ydb

package ydb

import asql "github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"

const (
	upsertQuery = `
		PRAGMA TablePathPrefix("%v");
		DECLARE $dir_hash AS int64;
		DECLARE $directory AS Utf8;
		DECLARE $name AS Utf8;
		DECLARE $meta AS String;
		DECLARE $expire_at AS Optional<uint32>;

		UPSERT INTO ` + asql.DEFAULT_TABLE + `
			(dir_hash, name, directory, meta, expire_at)
		VALUES
			($dir_hash, $name, $directory, $meta, $expire_at);`

	deleteQuery = `
		PRAGMA TablePathPrefix("%v");
		DECLARE $dir_hash AS int64;
		DECLARE $name AS Utf8;

		DELETE FROM ` + asql.DEFAULT_TABLE + ` 
		WHERE dir_hash = $dir_hash AND name = $name;`

	findQuery = `
		PRAGMA TablePathPrefix("%v");
		DECLARE $dir_hash AS int64;
		DECLARE $name AS Utf8;
		
		SELECT meta
		FROM ` + asql.DEFAULT_TABLE + ` 
		WHERE dir_hash = $dir_hash AND name = $name;`

	deleteFolderChildrenQuery = `
		PRAGMA TablePathPrefix("%v");
		DECLARE $dir_hash AS int64;
		DECLARE $directory AS Utf8;

		DELETE FROM ` + asql.DEFAULT_TABLE + ` 
		WHERE dir_hash = $dir_hash AND directory = $directory;`

	listDirectoryQuery = `
		PRAGMA TablePathPrefix("%v");
		DECLARE $dir_hash AS int64;
		DECLARE $directory AS Utf8;
		DECLARE $start_name AS Utf8;
		DECLARE $prefix AS Utf8;
		DECLARE $limit AS Uint64;
		
		SELECT name, meta
		FROM ` + asql.DEFAULT_TABLE + `
		WHERE dir_hash = $dir_hash AND directory = $directory and name > $start_name and name LIKE $prefix
		ORDER BY name ASC LIMIT $limit;`

	listInclusiveDirectoryQuery = `
		PRAGMA TablePathPrefix("%v");
		DECLARE $dir_hash AS int64;
		DECLARE $directory AS Utf8;
		DECLARE $start_name AS Utf8;
		DECLARE $prefix AS Utf8;
		DECLARE $limit AS Uint64;
		
		SELECT name, meta
		FROM ` + asql.DEFAULT_TABLE + `
		WHERE dir_hash = $dir_hash AND directory = $directory and name >= $start_name and name LIKE $prefix
		ORDER BY name ASC LIMIT $limit;`
)
