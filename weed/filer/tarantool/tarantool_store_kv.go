//go:build tarantool

package tarantool

import (
	"context"
	"fmt"
	"reflect"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/tarantool/go-option"
	"github.com/tarantool/go-tarantool/v3/crud"
	"github.com/tarantool/go-tarantool/v3/pool"
)

const (
	tarantoolKVSpaceName = "key_value"
)

func (store *TarantoolStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {

	var operations = []crud.Operation{
		{
			Operator: crud.Insert,
			Field:    "value",
			Value:    string(value),
		},
	}

	req := crud.NewUpsertRequest(tarantoolKVSpaceName).
		Tuple([]interface{}{string(key), nil, string(value)}).
		Operations(operations)

	ret := crud.Result{}
	if err := store.pool.Do(req, pool.ModeRW).GetTyped(&ret); err != nil {
		return fmt.Errorf("kv put: %w", err)
	}

	return nil
}

func (store *TarantoolStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {

	getOpts := crud.GetOpts{
		Fields:        option.SomeAny([]interface{}{"value"}),
		Mode:          option.SomeString("read"),
		PreferReplica: option.SomeBool(true),
		Balance:       option.SomeBool(true),
	}

	req := crud.NewGetRequest(tarantoolKVSpaceName).
		Key([]interface{}{string(key)}).
		Opts(getOpts)

	resp := crud.Result{}

	err = store.pool.Do(req, pool.ModePreferRO).GetTyped(&resp)
	if err != nil {
		return nil, err
	}

	results, ok := resp.Rows.([]interface{})
	if !ok || len(results) != 1 {
		return nil, filer.ErrKvNotFound
	}

	rows, ok := results[0].([]interface{})
	if !ok || len(rows) != 1 {
		return nil, filer.ErrKvNotFound
	}

	row, ok := rows[0].(string)
	if !ok {
		return nil, fmt.Errorf("Can't convert rows[0] field to string. Actual type: %v, value: %v", reflect.TypeOf(rows[0]), rows[0])
	}

	return []byte(row), nil
}

func (store *TarantoolStore) KvDelete(ctx context.Context, key []byte) (err error) {

	delOpts := crud.DeleteOpts{
		Noreturn: option.SomeBool(true),
	}

	req := crud.NewDeleteRequest(tarantoolKVSpaceName).
		Key([]interface{}{string(key)}).
		Opts(delOpts)

	if _, err := store.pool.Do(req, pool.ModeRW).Get(); err != nil {
		return fmt.Errorf("kv delete: %w", err)
	}

	return nil
}
