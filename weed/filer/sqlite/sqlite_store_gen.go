package sqlite

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer/mysql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
)

type SqlGenSqlite struct {
	mysql.SqlGenMysql
}

var (
	_ = abstract_sql.SqlGenerator(&SqlGenSqlite{})
)

func (gen *SqlGenSqlite) GetSqlListRecursive(tableName string) string {
	return fmt.Sprintf("SELECT `directory`, `name`, `meta` FROM `%s` WHERE `directory` || `name` > ? AND ((`dirhash` = ? AND `name` like ?) OR `directory` || `name` like ?) ORDER BY `directory` || `name` ASC LIMIT ?", tableName)
}
