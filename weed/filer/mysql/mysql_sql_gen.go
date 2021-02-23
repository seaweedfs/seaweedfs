package mysql

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer/abstract_sql"
	_ "github.com/go-sql-driver/mysql"
)

type SqlGenMysql struct {
	CreateTableSqlTemplate string
	DropTableSqlTemplate   string
}

var (
	_ = abstract_sql.SqlGenerator(&SqlGenMysql{})
)

func (gen *SqlGenMysql) GetSqlInsert(bucket string) string {
	return fmt.Sprintf("INSERT INTO `%s` (dirhash,name,directory,meta) VALUES(?,?,?,?)", bucket)
}

func (gen *SqlGenMysql) GetSqlUpdate(bucket string) string {
	return fmt.Sprintf("UPDATE `%s` SET meta=? WHERE dirhash=? AND name=? AND directory=?", bucket)
}

func (gen *SqlGenMysql) GetSqlFind(bucket string) string {
	return fmt.Sprintf("SELECT meta FROM `%s` WHERE dirhash=? AND name=? AND directory=?", bucket)
}

func (gen *SqlGenMysql) GetSqlDelete(bucket string) string {
	return fmt.Sprintf("DELETE FROM `%s` WHERE dirhash=? AND name=? AND directory=?", bucket)
}

func (gen *SqlGenMysql) GetSqlDeleteFolderChildren(bucket string) string {
	return fmt.Sprintf("DELETE FROM `%s` WHERE dirhash=? AND directory=?", bucket)
}

func (gen *SqlGenMysql) GetSqlListExclusive(bucket string) string {
	return fmt.Sprintf("SELECT NAME, meta FROM `%s` WHERE dirhash=? AND name>? AND directory=? AND name like ? ORDER BY NAME ASC LIMIT ?", bucket)
}

func (gen *SqlGenMysql) GetSqlListInclusive(bucket string) string {
	return fmt.Sprintf("SELECT NAME, meta FROM `%s` WHERE dirhash=? AND name>=? AND directory=? AND name like ? ORDER BY NAME ASC LIMIT ?", bucket)
}

func (gen *SqlGenMysql) GetSqlCreateTable(bucket string) string {
	return fmt.Sprintf(gen.CreateTableSqlTemplate, bucket)
}

func (gen *SqlGenMysql) GetSqlDropTable(bucket string) string {
	return fmt.Sprintf(gen.DropTableSqlTemplate, bucket)
}
