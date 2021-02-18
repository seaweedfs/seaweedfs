package postgres

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer/abstract_sql"
	_ "github.com/lib/pq"
)

type SqlGenPostgres struct {
	CreateTableSqlTemplate string
	DropTableSqlTemplate   string
}

var (
	_ = abstract_sql.SqlGenerator(&SqlGenPostgres{})
)

func (gen *SqlGenPostgres) GetSqlInsert(bucket string) string {
	return fmt.Sprintf(`INSERT INTO "%s" (dirhash,name,directory,meta) VALUES($1,$2,$3,$4)`, bucket)
}

func (gen *SqlGenPostgres) GetSqlUpdate(bucket string) string {
	return fmt.Sprintf(`UPDATE "%s" SET meta=$1 WHERE dirhash=$2 AND name=$3 AND directory=$4`, bucket)
}

func (gen *SqlGenPostgres) GetSqlFind(bucket string) string {
	return fmt.Sprintf(`SELECT meta FROM "%s" WHERE dirhash=$1 AND name=$2 AND directory=$3`, bucket)
}

func (gen *SqlGenPostgres) GetSqlDelete(bucket string) string {
	return fmt.Sprintf(`DELETE FROM "%s" WHERE dirhash=$1 AND name=$2 AND directory=$3`, bucket)
}

func (gen *SqlGenPostgres) GetSqlDeleteFolderChildren(bucket string) string {
	return fmt.Sprintf(`DELETE FROM "%s" WHERE dirhash=$1 AND directory=$2`, bucket)
}

func (gen *SqlGenPostgres) GetSqlListExclusive(bucket string) string {
	return fmt.Sprintf(`SELECT NAME, meta FROM "%s" WHERE dirhash=$1 AND name>$2 AND directory=$3 AND name like $4 ORDER BY NAME ASC LIMIT $5`, bucket)
}

func (gen *SqlGenPostgres) GetSqlListInclusive(bucket string) string {
	return fmt.Sprintf(`SELECT NAME, meta FROM "%s" WHERE dirhash=$1 AND name>=$2 AND directory=$3 AND name like $4 ORDER BY NAME ASC LIMIT $5`, bucket)
}

func (gen *SqlGenPostgres) GetSqlCreateTable(bucket string) string {
	return fmt.Sprintf(gen.CreateTableSqlTemplate, bucket)
}

func (gen *SqlGenPostgres) GetSqlDropTable(bucket string) string {
	return fmt.Sprintf(gen.DropTableSqlTemplate, bucket)
}
