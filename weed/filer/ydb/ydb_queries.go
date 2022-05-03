package ydb

import asql "github.com/chrislusf/seaweedfs/weed/filer/abstract_sql"

const (
	insertQuery = `
		DECLARE $dir_hash AS int64;
		DECLARE $directory AS Utf8;
		DECLARE $name AS Utf8;
		DECLARE $meta AS String;

		UPSERT INTO ` + asql.DEFAULT_TABLE + `
			(dir_hash, name, directory, meta)
		VALUES
			($dir_hash, $name, $directory, $meta);`

	updateQuery = `
		DECLARE $dir_hash AS int64;
		DECLARE $directory AS Utf8;
		DECLARE $name AS Utf8;
		DECLARE $meta AS String;

		REPLACE INTO ` + asql.DEFAULT_TABLE + `
			(dir_hash, name, directory, meta)
		VALUES
			($dir_hash, $name, $directory, $meta);`

	deleteQuery = `
		DECLARE $dir_hash AS int64;
		DECLARE $name AS Utf8;

		DELETE FROM ` + asql.DEFAULT_TABLE + ` 
		WHERE dir_hash = $dir_hash AND name = $name;`

	findQuery = `
		DECLARE $dir_hash AS int64;
		DECLARE $name AS Utf8;
		
		SELECT meta
		FROM ` + asql.DEFAULT_TABLE + ` 
		WHERE dir_hash = $dir_hash AND name = $name;`

	deleteFolderChildrenQuery = `
		DECLARE $dir_hash AS int64;
		DECLARE $directory AS Utf8;

		DELETE FROM ` + asql.DEFAULT_TABLE + ` 
		WHERE dir_hash = $dir_hash AND directory = $directory;`

	listDirectoryQuery = `
		DECLARE $dir_hash AS int64;
		DECLARE $directory AS Utf8;
		DECLARE $start_name AS Utf8;
		DECLARE $prefix AS Utf8;
		DECLARE $limit AS Uint64;
		
		SELECT name, meta
		FROM ` + asql.DEFAULT_TABLE + `
		WHERE dir_hash = $dir_hash AND directory = $directory and name %s $start_name and name LIKE $prefix
		ORDER BY name ASC LIMIT $limit;`
)
