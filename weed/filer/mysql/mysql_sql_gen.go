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
	// Force byte ordering on a non-binary name column; see ConfigureListOrdering.
	ForceBinaryCollation bool
}

// DefaultUpsertQuery keeps INSERTs idempotent so the inode-index KvPut
// after every entry write does not emit a duplicate-key roundtrip on
// every mutation. The VALUES() form works on MariaDB and MySQL >=5.7;
// the newer "AS new" alias from MySQL 8.0.19 errors on MariaDB, so it
// is left for users to opt into via an explicit upsertQuery.
const DefaultUpsertQuery = "INSERT INTO `%s` (`dirhash`,`name`,`directory`,`meta`) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE `meta` = VALUES(`meta`)"

// DefaultCreateTableQuery is used when createTable is left unset; an empty
// template would otherwise render as %!(EXTRA ...) garbage SQL.
const DefaultCreateTableQuery = "CREATE TABLE IF NOT EXISTS `%s` (`dirhash` BIGINT NOT NULL, `name` VARCHAR(766) NOT NULL, `directory` TEXT NOT NULL, `meta` LONGBLOB, PRIMARY KEY (`dirhash`, `name`)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"

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

// nameExpr forces byte ordering on a non-binary column at the cost of a filesort.
func (gen *SqlGenMysql) nameExpr() string {
	if gen.ForceBinaryCollation {
		return "BINARY `name`"
	}
	return "`name`"
}

// GetSqlScanPage spells the cursor as two primary-key ranges: MySQL 5.7 does
// not turn a row-value comparison into a range seek. The name keeps the
// column's own collation (BINARY would filesort every page); the scan promises
// no lexical order. SQLite overrides this method.
func (gen *SqlGenMysql) GetSqlScanPage(tableName string, dirhash int64, name string, limit int) (string, []interface{}) {
	return fmt.Sprintf("SELECT `dirhash`, `name`, `directory`, `meta` FROM `%s` WHERE `dirhash` > ? OR (`dirhash` = ? AND `name` > ?) ORDER BY `dirhash` ASC, `name` ASC LIMIT ?", tableName),
		[]interface{}{dirhash, dirhash, name, limit}
}

func (gen *SqlGenMysql) GetSqlListExclusive(tableName string) string {
	name := gen.nameExpr()
	return fmt.Sprintf("SELECT `name`, `meta` FROM `%s` WHERE `dirhash` = ? AND %s > ? AND `directory` = ? AND %s LIKE ? ESCAPE '!' ORDER BY %s ASC LIMIT ?", tableName, name, name, name)
}

func (gen *SqlGenMysql) GetSqlListInclusive(tableName string) string {
	name := gen.nameExpr()
	return fmt.Sprintf("SELECT `name`, `meta` FROM `%s` WHERE `dirhash` = ? AND %s >= ? AND `directory` = ? AND %s LIKE ? ESCAPE '!' ORDER BY %s ASC LIMIT ?", tableName, name, name, name)
}

func (gen *SqlGenMysql) GetSqlCreateTable(tableName string) string {
	if gen.CreateTableSqlTemplate == "" {
		return ""
	}
	return fmt.Sprintf(gen.CreateTableSqlTemplate, tableName)
}

func (gen *SqlGenMysql) GetSqlDropTable(tableName string) string {
	return fmt.Sprintf(gen.DropTableSqlTemplate, tableName)
}
