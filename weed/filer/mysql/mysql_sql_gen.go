package mysql

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
)

type SqlGenMysql struct {
	CreateTableSqlTemplate string
	DropTableSqlTemplate   string
	UpsertQueryTemplate    string
}

var (
	_ = abstract_sql.SqlGenerator(&SqlGenMysql{})
)

func (gen *SqlGenMysql) GetSqlInsert(tableName string) string {
	if gen.UpsertQueryTemplate != "" {
		return fmt.Sprintf(gen.UpsertQueryTemplate, tableName)
	} else {
		return fmt.Sprintf("INSERT INTO `%s` (`dirhash`,`name`,`directory`,`meta`) VALUES(?,?,?,?)", tableName)
	}
}

func (gen *SqlGenMysql) GetSqlUpdate(tableName string) string {
	return fmt.Sprintf("UPDATE `%s` SET `meta` = ? WHERE `dirhash` = ? AND `name` = ? AND `directory` = ?", tableName)
}

func (gen *SqlGenMysql) GetSqlFind(tableName string) string {
	return fmt.Sprintf("SELECT `meta` FROM `%s` WHERE `dirhash` = ? AND `name` = ? AND `directory` = ?", tableName)
}

func (gen *SqlGenMysql) GetSqlDelete(tableName string) string {
	return fmt.Sprintf("DELETE FROM `%s` WHERE `dirhash` = ? AND `name` = ? AND `directory` = ?", tableName)
}

func (gen *SqlGenMysql) GetSqlDeleteFolderChildren(tableName string) string {
	return fmt.Sprintf("DELETE FROM `%s` WHERE `dirhash` = ? AND `directory` = ?", tableName)
}

func (gen *SqlGenMysql) GetSqlListExclusive(tableName string) string {
	return fmt.Sprintf("SELECT `name`, `meta` FROM `%s` WHERE `dirhash` = ? AND `name` > ? AND `directory` = ? AND `name` LIKE ? ORDER BY `name` ASC LIMIT ?", tableName)
}

func (gen *SqlGenMysql) GetSqlListInclusive(tableName string) string {
	return fmt.Sprintf("SELECT `name`, `meta` FROM `%s` WHERE `dirhash` = ? AND `name` >= ? AND `directory` = ? AND `name` LIKE ? ORDER BY `name` ASC LIMIT ?", tableName)
}

func (gen *SqlGenMysql) GetSqlCreateTable(tableName string) string {
	return fmt.Sprintf(gen.CreateTableSqlTemplate, tableName)
}

func (gen *SqlGenMysql) GetSqlDropTable(tableName string) string {
	return fmt.Sprintf(gen.DropTableSqlTemplate, tableName)
}
