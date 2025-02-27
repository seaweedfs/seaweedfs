//go:build dameng
// +build dameng

package dameng

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
)

type SqlGenDameng struct {
	CreateTableSqlTemplate string
	DropTableSqlTemplate   string
	UpsertQueryTemplate    string
}

var (
	_ = abstract_sql.SqlGenerator(&SqlGenDameng{})
)

func (gen *SqlGenDameng) GetSqlInsert(tableName string) string {
	sql := ""
	if gen.UpsertQueryTemplate != "" {
		sql = fmt.Sprintf(`MERGE INTO %s AS target
			USING (SELECT ? AS dirhash, ? AS name, ? AS directory, ? AS meta FROM dual) AS source
			ON (target.dirhash = source.dirhash AND target.name = source.name)
			WHEN MATCHED THEN
			  UPDATE SET target.meta = source.meta
			WHEN NOT MATCHED THEN
			  INSERT (dirhash, name, directory, meta)
			  VALUES (source.dirhash, source.name, source.directory, source.meta);`, tableName)
	} else {
		sql = fmt.Sprintf("INSERT INTO %s (dirhash,name,directory,meta) VALUES(?,?,?,?)", tableName)
	}
	return sql
}

func (gen *SqlGenDameng) GetSqlUpdate(tableName string) string {
	return fmt.Sprintf("UPDATE %s SET meta = ? WHERE dirhash = ? AND name = ? AND directory = ?", tableName)
}

func (gen *SqlGenDameng) GetSqlFind(tableName string) string {
	return fmt.Sprintf("SELECT meta FROM %s WHERE dirhash = ? AND name = ? AND directory = ?", tableName)
}

func (gen *SqlGenDameng) GetSqlDelete(tableName string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE dirhash = ? AND name = ? AND directory = ?", tableName)
}

func (gen *SqlGenDameng) GetSqlDeleteFolderChildren(tableName string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE dirhash = ? AND directory = ?", tableName)
}

func (gen *SqlGenDameng) GetSqlListExclusive(tableName string) string {
	return fmt.Sprintf("SELECT name, meta FROM %s WHERE dirhash = ? AND rowid > (SELECT IFNULL(MIN(rowid), 0) FROM %s WHERE directory = ? AND name = ?) AND directory = ? ORDER BY rowid ASC LIMIT ?", tableName, tableName)
}

func (gen *SqlGenDameng) GetSqlListInclusive(tableName string) string {
	return fmt.Sprintf("SELECT name, meta FROM %s WHERE dirhash = ? AND rowid >= (SELECT IFNULL(MIN(rowid), 0) FROM %s WHERE directory = ? AND name = ?) AND directory = ? ORDER BY rowid ASC LIMIT ?", tableName, tableName)
}

func (gen *SqlGenDameng) GetSqlCreateTable(tableName string) string {
	return fmt.Sprintf(gen.CreateTableSqlTemplate, tableName)
}

func (gen *SqlGenDameng) GetSqlDropTable(tableName string) string {
	return fmt.Sprintf(gen.DropTableSqlTemplate, tableName)
}
