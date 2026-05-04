package unity_catalog

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/seaweedfs/seaweedfs/test/testutil"
)

// TestUnityCatalogMasterRoleIntegration is the role-vended counterpart of
// TestUnityCatalogDeltaIntegration. It boots SeaweedFS with an
// IAM/STS-enabled config (mirroring the lakekeeper test) and points Unity
// Catalog at it via:
//
//   - aws.masterRoleArn = arn:aws:iam::000000000000:role/UnityCatalogVendedRole
//   - AWS_ENDPOINT_URL_STS / AWS_ENDPOINT_URL env vars pointed at SeaweedFS
//
// The test passes when Unity Catalog returns aws_temp_credentials whose
// session_token is non-empty (proof that UC actually performed sts:AssumeRole
// against SeaweedFS rather than echoing the static keys back), and those
// vended credentials successfully round-trip an object on SeaweedFS S3.
//
// This is the configuration users would need to run Unity Catalog against
// SeaweedFS in production, and it covers the gap that the upstream playground
// notes as not-yet-working: <https://github.com/data-engineering-helpers/mds-in-a-box/blob/main/unitycatalog-playground/etc/conf/server.properties#L45>.
func TestUnityCatalogMasterRoleIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	if !testutil.HasDocker() {
		t.Skip("docker not available")
	}

	env := newTestEnv(t)
	defer env.cleanup(t)

	t.Log(">>> starting SeaweedFS with STS-enabled IAM config...")
	env.startSeaweedFS(t, stsEnabledIAMConfig(env.accessKey, env.secretKey))
	t.Log(">>> SeaweedFS ready")

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	s3c := env.newHostS3Client(t, ctx)
	if _, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(ucWarehouse)}); err != nil {
		t.Fatalf("create warehouse bucket: %v", err)
	}

	stsEndpoint := fmt.Sprintf("http://host.docker.internal:%d", env.s3Port)

	t.Log(">>> starting Unity Catalog server (master role)...")
	env.startUnityCatalog(t, ctx, ucServerOpts{
		MasterRoleArn: ucVendedRoleArn,
		ExtraEnv: map[string]string{
			// AWS SDK v2 honors these for routing the StsClient at SeaweedFS
			// instead of the real AWS STS endpoint. Both are set so older
			// SDK versions that only honor AWS_ENDPOINT_URL also work.
			"AWS_ENDPOINT_URL":     stsEndpoint,
			"AWS_ENDPOINT_URL_STS": stsEndpoint,
			"AWS_ACCESS_KEY_ID":    env.accessKey,
			"AWS_SECRET_ACCESS_KEY": env.secretKey,
			"AWS_REGION":           "us-east-1",
		},
	})
	t.Log(">>> Unity Catalog ready")

	uc := newUCClient(fmt.Sprintf("http://127.0.0.1:%d", env.ucHostPort))

	suffix := time.Now().UnixNano()
	catalogName := fmt.Sprintf("seaweed_uc_role_%d", suffix)
	schemaName := fmt.Sprintf("ns_%d", suffix)
	tableName := fmt.Sprintf("events_%d", suffix)
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
		},
		StorageLocation: tableLocation,
	})
	if err != nil {
		t.Fatalf("create external delta table: %v", err)
	}
	if created.TableID == "" {
		t.Fatalf("expected table_id in CreateTable response, got %+v", created)
	}

	t.Run("StsVendedCredentialsHaveSessionToken", func(t *testing.T) {
		creds, err := uc.generateTemporaryTableCredentials(ctx, created.TableID, "READ_WRITE")
		if err != nil {
			t.Fatalf("temporary-table-credentials (master role): %v", err)
		}
		if creds.AwsTempCredentials == nil {
			t.Fatalf("expected aws_temp_credentials, got %+v", creds)
		}
		if creds.AwsTempCredentials.SessionToken == "" {
			t.Fatalf("session_token empty in vended creds; UC did not exercise the sts:AssumeRole path. creds=%+v", creds.AwsTempCredentials)
		}
		if creds.AwsTempCredentials.AccessKeyID == env.accessKey {
			t.Fatalf("vended access key matches static admin key; UC echoed static creds instead of assuming the role")
		}

		// Round-trip an object using the vended creds.
		s3v := env.newHostS3ClientWithCreds(t, ctx,
			creds.AwsTempCredentials.AccessKeyID,
			creds.AwsTempCredentials.SecretAccessKey,
			creds.AwsTempCredentials.SessionToken)

		bucket, key := splitS3URI(t, tableLocation)
		probeKey := key + "/_delta_log/00000000000000000000.json.probe"
		body := []byte(`{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}`)
		if _, err := s3v.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(probeKey),
			Body:   bytes.NewReader(body),
		}); err != nil {
			t.Fatalf("PutObject with sts-vended creds: %v", err)
		}
		got, err := s3v.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(probeKey),
		})
		if err != nil {
			t.Fatalf("GetObject with sts-vended creds: %v", err)
		}
		read, err := io.ReadAll(got.Body)
		_ = got.Body.Close()
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if !bytes.Equal(read, body) {
			t.Fatalf("body mismatch: got %q want %q", read, body)
		}
		_, _ = s3v.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(probeKey),
		})
	})
}
