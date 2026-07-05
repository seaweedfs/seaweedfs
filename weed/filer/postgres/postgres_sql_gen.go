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
	// Force byte ordering on a locale-aware name column; see ConfigureListOrdering.
	ForceBinaryCollation bool
}

// DefaultUpsertQuery keeps INSERTs idempotent so a duplicate-key failure
// (23505) cannot poison the surrounding transaction (25P02). Used when the
// user enables upsert but does not provide their own template.
const DefaultUpsertQuery = `INSERT INTO "%s" (dirhash,name,directory,meta) VALUES($1,$2,$3,$4) ON CONFLICT (dirhash, name) DO UPDATE SET directory=EXCLUDED.directory, meta=EXCLUDED.meta`

// DefaultCreateTableQuery is used when createTable is left unset; an empty
// template would otherwise render as %!(EXTRA ...) garbage SQL.
const DefaultCreateTableQuery = `CREATE TABLE IF NOT EXISTS "%s" (dirhash BIGINT, name VARCHAR(65535), directory VARCHAR(65535), meta bytea, PRIMARY KEY (dirhash, name))`

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

// nameExpr forces byte ordering on a locale-aware column via COLLATE "C".
func (gen *SqlGenPostgres) nameExpr() string {
	if gen.ForceBinaryCollation {
		return `name COLLATE "C"`
	}
	return "name"
}

func (gen *SqlGenPostgres) GetSqlListExclusive(tableName string) string {
	name := gen.nameExpr()
	return fmt.Sprintf(`SELECT NAME, meta FROM "%s" WHERE dirhash=$1 AND %s>$2 AND directory=$3 AND %s like $4 ORDER BY %s ASC LIMIT $5`, tableName, name, name, name)
}

func (gen *SqlGenPostgres) GetSqlListInclusive(tableName string) string {
	name := gen.nameExpr()
	return fmt.Sprintf(`SELECT NAME, meta FROM "%s" WHERE dirhash=$1 AND %s>=$2 AND directory=$3 AND %s like $4 ORDER BY %s ASC LIMIT $5`, tableName, name, name, name)
}

func (gen *SqlGenPostgres) GetSqlCreateTable(tableName string) string {
	return fmt.Sprintf(gen.CreateTableSqlTemplate, tableName)
}

func (gen *SqlGenPostgres) GetSqlDropTable(tableName string) string {
	return fmt.Sprintf(gen.DropTableSqlTemplate, tableName)
}
