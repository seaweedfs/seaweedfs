package postgres_s3

/*
 * Copyright 2022 Splunk Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"context"
	"database/sql"
	"fmt"
	"path"

	"github.com/lib/pq"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
)

func (store *PostgresS3Store) KvPut(ctx context.Context, key []byte, value []byte) error {
	db, _, _, err := store.getTxOrDB(ctx, "", false)
	if err != nil {
		return fmt.Errorf("findDB: %v", err)
	}

	prefixes := calculatePrefixes(string(key))
	hashedPrefixes := hashPrefixArray(prefixes)
	_, err = db.ExecContext(ctx, fmt.Sprintf(insertEntryPattern, abstract_sql.DEFAULT_TABLE), key, path.Base(string(key)), pq.Array(hashedPrefixes), value)
	if err != nil {
		return fmt.Errorf("kv put: %v", err)
	}
	return nil
}

func (store *PostgresS3Store) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	db, _, _, err := store.getTxOrDB(ctx, "", false)
	if err != nil {
		return nil, fmt.Errorf("findDB: %v", err)
	}

	row := db.QueryRowContext(ctx, fmt.Sprintf(findEntryPattern, abstract_sql.DEFAULT_TABLE), key)

	err = row.Scan(&value)

	if err == sql.ErrNoRows {
		return nil, filer.ErrKvNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("kv get: %v", err)
	}

	return
}

func (store *PostgresS3Store) KvDelete(ctx context.Context, key []byte) error {
	db, _, _, err := store.getTxOrDB(ctx, "", false)
	if err != nil {
		return fmt.Errorf("findDB: %v", err)
	}

	res, err := db.ExecContext(ctx, fmt.Sprintf(deleteEntryPattern, abstract_sql.DEFAULT_TABLE), key)
	if err != nil {
		return fmt.Errorf("kv delete: %s", err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("kv delete no rows affected: %s", err)
	}

	return nil
}
