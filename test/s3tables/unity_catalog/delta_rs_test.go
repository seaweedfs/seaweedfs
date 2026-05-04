package unity_catalog

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/seaweedfs/seaweedfs/test/testutil"
)

// TestUnityCatalogDeltaRsRoundTrip writes and reads a real Delta table
// against the SeaweedFS-backed UC warehouse using the delta-rs Python
// library inside a Docker container. The table is registered in UC as
// EXTERNAL/DELTA, the script fetches temporary table credentials from UC,
// and uses them as delta-rs storage_options to perform a write+read.
//
// This is the Spark/Delta-RS slice that the earlier test scaffold listed
// as not-done; it doesn't pull in a full Spark runtime, so it stays fast
// while still exercising a real Delta client.
func TestUnityCatalogDeltaRsRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	if !testutil.HasDocker() {
		t.Skip("docker not available")
	}

	env := newTestEnv(t)
	defer env.cleanup(t)

	t.Log(">>> starting SeaweedFS for delta-rs test...")
	env.startSeaweedFS(t, "")
	t.Log(">>> SeaweedFS ready")

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	s3c := env.newHostS3Client(t, ctx)
	if _, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(ucWarehouse)}); err != nil {
		t.Fatalf("create warehouse bucket: %v", err)
	}

	t.Log(">>> starting Unity Catalog server...")
	env.startUnityCatalog(t, ctx, ucServerOpts{})
	t.Log(">>> Unity Catalog ready")

	uc := newUCClient(fmt.Sprintf("http://127.0.0.1:%d", env.ucHostPort))

	suffix := time.Now().UnixNano()
	catalogName := fmt.Sprintf("seaweed_uc_delta_rs_%d", suffix)
	schemaName := fmt.Sprintf("ns_%d", suffix)
	tableName := fmt.Sprintf("delta_rs_%d", suffix)
	tableLocation := fmt.Sprintf("s3://%s/%s/%s/%s", ucWarehouse, ucWarehouseKey, schemaName, tableName)

	defer func() {
		_ = uc.deleteTable(context.Background(), catalogName+"."+schemaName+"."+tableName)
		_ = uc.deleteSchema(context.Background(), catalogName+"."+schemaName)
		_ = uc.deleteCatalog(context.Background(), catalogName)
	}()

	if _, err := uc.createCatalog(ctx, ucCreateCatalog{
		Name:        catalogName,
		StorageRoot: fmt.Sprintf("s3://%s/%s", ucWarehouse, ucWarehouseKey),
	}); err != nil {
		t.Fatalf("create catalog: %v", err)
	}
	if _, err := uc.createSchema(ctx, ucCreateSchema{Name: schemaName, CatalogName: catalogName}); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	created, err := uc.createTable(ctx, ucCreateTable{
		Name:             tableName,
		CatalogName:      catalogName,
		SchemaName:       schemaName,
		TableType:        "EXTERNAL",
		DataSourceFormat: "DELTA",
		Columns: []ucColumn{
			{Name: "id", TypeText: "long", TypeName: "LONG", TypeJSON: `{"name":"id","type":"long","nullable":true,"metadata":{}}`, Position: 0, Nullable: true},
			{Name: "value", TypeText: "string", TypeName: "STRING", TypeJSON: `{"name":"value","type":"string","nullable":true,"metadata":{}}`, Position: 1, Nullable: true},
		},
		StorageLocation: tableLocation,
	})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	testDir, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	imageTag := fmt.Sprintf("seaweed-uc-delta-rs:%d", time.Now().UnixNano())

	build := exec.CommandContext(ctx, "docker", "build", "-t", imageTag, "-f", "Dockerfile.delta-rs", ".")
	build.Dir = testDir
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("docker build delta-rs image: %v\n%s", err, out)
	}
	defer func() { _ = exec.Command("docker", "rmi", "-f", imageTag).Run() }()

	ucURLForContainer := fmt.Sprintf("http://host.docker.internal:%d", env.ucHostPort)
	s3EndpointForContainer := fmt.Sprintf("http://host.docker.internal:%d", env.s3Port)

	cmd := exec.CommandContext(ctx, "docker", "run", "--rm",
		"--add-host", "host.docker.internal:host-gateway",
		"-e", "UC_URL="+ucURLForContainer,
		"-e", "S3_ENDPOINT="+s3EndpointForContainer,
		"-e", "UC_CATALOG="+catalogName,
		"-e", "UC_SCHEMA="+schemaName,
		"-e", "UC_TABLE="+tableName,
		"-e", "UC_TABLE_ID="+created.TableID,
		imageTag,
	)
	cmd.Stdout = os.Stdout
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("delta-rs container failed: %v\nstdout:\n%s", err, out)
	}
	got := string(out)
	if !strings.Contains(got, "DELTA_RS_OK rows=3") {
		t.Fatalf("expected DELTA_RS_OK rows=3 in output; got:\n%s", got)
	}
	t.Logf("delta-rs output: %s", strings.TrimSpace(got))
}
