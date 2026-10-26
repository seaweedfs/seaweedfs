package catalog_spark

import (
	"fmt"
	"strings"
	"testing"
)

// TestSparkPartitionByFirstColumn creates tables partitioned by their first
// column. Spark (Iceberg Java) numbers the columns of a new table from 0 and
// leaves assigning fresh IDs to the catalog, so these partition specs arrive
// with source-id 0, which the catalog used to reject as a malformed request.
func TestSparkPartitionByFirstColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env, _, _ := setupSparkTestEnv(t)

	namespace := "spark_partition_" + randomString(6)
	output := runSparkPySQL(t, env.sparkContainer, fmt.Sprintf(`
spark.sql("CREATE NAMESPACE iceberg.%s")
print("Namespace created")
`, namespace), env.icebergRestPort, env.s3Port)
	if !strings.Contains(output, "Namespace created") {
		t.Fatalf("namespace creation failed, output: %s", output)
	}

	cases := []struct {
		name           string
		columns        string
		partitionBy    string
		values         string
		wantField      string
		wantPartitions int
	}{
		{
			name:           "identity",
			columns:        "id INT, name STRING",
			partitionBy:    "id",
			values:         "(1, 'a'), (2, 'b'), (3, 'c')",
			wantField:      "id",
			wantPartitions: 3,
		},
		{
			name:           "bucket",
			columns:        "id BIGINT, name STRING",
			partitionBy:    "bucket(1, id)",
			values:         "(1, 'a'), (2, 'b'), (3, 'c')",
			wantField:      "id_bucket",
			wantPartitions: 1,
		},
		{
			name:           "days",
			columns:        "ts TIMESTAMP, name STRING",
			partitionBy:    "days(ts)",
			values:         "(TIMESTAMP '2026-01-01 10:00:00', 'a'), (TIMESTAMP '2026-01-01 11:00:00', 'b'), (TIMESTAMP '2026-01-02 10:00:00', 'c')",
			wantField:      "ts_day",
			wantPartitions: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tableName := fmt.Sprintf("iceberg.%s.first_col_%s", namespace, tc.name)
			output := runSparkPySQL(t, env.sparkContainer, fmt.Sprintf(`
spark.sql("CREATE TABLE %[1]s (%[2]s) USING iceberg PARTITIONED BY (%[3]s)")
spark.sql("INSERT INTO %[1]s VALUES %[4]s")
count = spark.sql("SELECT COUNT(*) AS count FROM %[1]s").collect()[0]['count']
print(f"Row count: {count}")
partitions = spark.table("%[1]s.partitions")
print(f"Partition count: {partitions.count()}")
print("Partition fields: " + ",".join(f.name for f in partitions.schema["partition"].dataType.fields))
`, tableName, tc.columns, tc.partitionBy, tc.values), env.icebergRestPort, env.s3Port)

			if !strings.Contains(output, "Row count: 3") {
				t.Fatalf("create/insert on a table partitioned by %s failed, output: %s", tc.partitionBy, output)
			}
			if want := fmt.Sprintf("Partition count: %d", tc.wantPartitions); !strings.Contains(output, want) {
				t.Errorf("expected %q, output: %s", want, output)
			}
			if want := "Partition fields: " + tc.wantField; !strings.Contains(output, want) {
				t.Errorf("expected %q, output: %s", want, output)
			}
		})
	}
}
