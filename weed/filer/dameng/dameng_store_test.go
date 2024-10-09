package dameng

import (
	"github.com/seaweedfs/seaweedfs/weed/filer/store_test"
	"testing"
)

func TestStore(t *testing.T) {
	// run "make test_ydb" under docker folder.
	// to set up local env
	store := &DamengStore{}
	store.initialize("localhost", `MERGE INTO %s AS target
USING (SELECT ? AS dirhash, ? AS name, ? AS directory, ? AS meta FROM dual) AS source
ON (target.dirhash = source.dirhash AND target.name = source.name)
WHEN MATCHED THEN
  UPDATE SET target.meta = source.meta
WHEN NOT MATCHED THEN
  INSERT (dirhash, name, directory, meta)
  VALUES (source.dirhash, source.name, source.directory, source.meta);`,
		true, "SYSDBA", "SYSDBA001", "localhost", 5236,
		"seaweedfs", 100, 2, 10, false)
	store_test.TestFilerStore(t, store)
}
