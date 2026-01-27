package postgres

import (
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
)

type SqlGenPostgres struct {
	CreateTableSqlTemplate string
	DropTableSqlTemplate   string
	UpsertQueryTemplate    string
	Schema                 string
}

var (
	_ = abstract_sql.SqlGenerator(&SqlGenPostgres{})
)

func (gen *SqlGenPostgres) getTableName(tableName string) string {
	if gen.Schema != "" {
		return fmt.Sprintf(`"%s"."%s"`, gen.Schema, tableName)
	}
	return fmt.Sprintf(`"%s"`, tableName)
}

func (gen *SqlGenPostgres) GetSqlInsert(tableName string) string {
	if gen.UpsertQueryTemplate != "" {
		return fmt.Sprintf(gen.UpsertQueryTemplate, gen.getTableName(tableName))
	} else {
		return fmt.Sprintf(`INSERT INTO %s (dirhash,name,directory,meta) VALUES($1,$2,$3,$4)`, gen.getTableName(tableName))
	}
}

func (gen *SqlGenPostgres) GetSqlUpdate(tableName string) string {
	return fmt.Sprintf(`UPDATE %s SET meta=$1 WHERE dirhash=$2 AND name=$3 AND directory=$4`, gen.getTableName(tableName))
}

func (gen *SqlGenPostgres) GetSqlFind(tableName string) string {
	return fmt.Sprintf(`SELECT meta FROM %s WHERE dirhash=$1 AND name=$2 AND directory=$3`, gen.getTableName(tableName))
}

func (gen *SqlGenPostgres) GetSqlDelete(tableName string) string {
	return fmt.Sprintf(`DELETE FROM %s WHERE dirhash=$1 AND name=$2 AND directory=$3`, gen.getTableName(tableName))
}

func (gen *SqlGenPostgres) GetSqlDeleteFolderChildren(tableName string) string {
	return fmt.Sprintf(`DELETE FROM %s WHERE dirhash=$1 AND directory=$2`, gen.getTableName(tableName))
}

func (gen *SqlGenPostgres) GetSqlListExclusive(tableName string) string {
	return fmt.Sprintf(`SELECT NAME, meta FROM %s WHERE dirhash=$1 AND name>$2 AND directory=$3 AND name like $4 ORDER BY NAME ASC LIMIT $5`, gen.getTableName(tableName))
}

func (gen *SqlGenPostgres) GetSqlListInclusive(tableName string) string {
	return fmt.Sprintf(`SELECT NAME, meta FROM %s WHERE dirhash=$1 AND name>=$2 AND directory=$3 AND name like $4 ORDER BY NAME ASC LIMIT $5`, gen.getTableName(tableName))
}

func (gen *SqlGenPostgres) GetSqlCreateTable(tableName string) string {
	return fmt.Sprintf(gen.CreateTableSqlTemplate, gen.getTableName(tableName))
}

func (gen *SqlGenPostgres) GetSqlDropTable(tableName string) string {
	return fmt.Sprintf(gen.DropTableSqlTemplate, gen.getTableName(tableName))
}
