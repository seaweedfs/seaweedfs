package postgres

import (
	"fmt"

	_ "github.com/lib/pq"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
)

type SqlGenPostgres struct {
	CreateTableSqlTemplate string
	DropTableSqlTemplate   string
	UpsertQueryTemplate    string
}

var (
	_ = abstract_sql.SqlGenerator(&SqlGenPostgres{})
)

func (gen *SqlGenPostgres) GetSqlInsert(tableName string) string {
	if gen.UpsertQueryTemplate != "" {
		return fmt.Sprintf(gen.UpsertQueryTemplate, tableName)
	} else {
		return fmt.Sprintf(`INSERT INTO "%s" (dirhash,name,directory,meta) VALUES($1,$2,$3,$4)`, tableName)
	}
}

func (gen *SqlGenPostgres) GetSqlUpdate(tableName string) string {
	return fmt.Sprintf(`UPDATE "%s" SET meta=$1 WHERE dirhash=$2 AND name=$3 AND directory=$4`, tableName)
}

func (gen *SqlGenPostgres) GetSqlFind(tableName string) string {
	return fmt.Sprintf(`SELECT meta FROM "%s" WHERE dirhash=$1 AND name=$2 AND directory=$3`, tableName)
}

func (gen *SqlGenPostgres) GetSqlDelete(tableName string) string {
	return fmt.Sprintf(`DELETE FROM "%s" WHERE dirhash=$1 AND name=$2 AND directory=$3`, tableName)
}

func (gen *SqlGenPostgres) GetSqlDeleteFolderChildren(tableName string) string {
	return fmt.Sprintf(`DELETE FROM "%s" WHERE dirhash=$1 AND directory=$2`, tableName)
}

func (gen *SqlGenPostgres) GetSqlListExclusive(tableName string) string {
	return fmt.Sprintf(`SELECT NAME, meta FROM "%s" WHERE dirhash=$1 AND name>$2 AND directory=$3 AND name like $4 ORDER BY NAME ASC LIMIT $5`, tableName)
}

func (gen *SqlGenPostgres) GetSqlListInclusive(tableName string) string {
	return fmt.Sprintf(`SELECT NAME, meta FROM "%s" WHERE dirhash=$1 AND name>=$2 AND directory=$3 AND name like $4 ORDER BY NAME ASC LIMIT $5`, tableName)
}

func (gen *SqlGenPostgres) GetSqlCreateTable(tableName string) string {
	return fmt.Sprintf(gen.CreateTableSqlTemplate, tableName)
}

func (gen *SqlGenPostgres) GetSqlDropTable(tableName string) string {
	return fmt.Sprintf(gen.DropTableSqlTemplate, tableName)
}
