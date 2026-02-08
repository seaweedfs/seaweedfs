package dash

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// S3Tables data structures for admin UI

type S3TablesBucketsData struct {
	Username     string                  `json:"username"`
	Buckets      []S3TablesBucketSummary `json:"buckets"`
	TotalBuckets int                     `json:"total_buckets"`
	IcebergPort  int                     `json:"iceberg_port"`
	LastUpdated  time.Time               `json:"last_updated"`
}

type S3TablesBucketSummary struct {
	ARN            string    `json:"arn"`
	Name           string    `json:"name"`
	OwnerAccountID string    `json:"ownerAccountId"`
	CreatedAt      time.Time `json:"createdAt"`
}

type S3TablesNamespacesData struct {
	Username        string                      `json:"username"`
	BucketARN       string                      `json:"bucket_arn"`
	Namespaces      []s3tables.NamespaceSummary `json:"namespaces"`
	TotalNamespaces int                         `json:"total_namespaces"`
	LastUpdated     time.Time                   `json:"last_updated"`
}

type S3TablesTablesData struct {
	Username    string                  `json:"username"`
	BucketARN   string                  `json:"bucket_arn"`
	Namespace   string                  `json:"namespace"`
	Tables      []s3tables.TableSummary `json:"tables"`
	TotalTables int                     `json:"total_tables"`
	LastUpdated time.Time               `json:"last_updated"`
}

type tableBucketMetadata struct {
	Name           string    `json:"name"`
	CreatedAt      time.Time `json:"createdAt"`
	OwnerAccountID string    `json:"ownerAccountId"`
}

// S3Tables manager helpers

const s3TablesAdminListLimit = 1000

func newS3TablesManager() *s3tables.Manager {
	manager := s3tables.NewManager()
	manager.SetAccountID(s3_constants.AccountAdminId)
	return manager
}

func (s *AdminServer) executeS3TablesOperation(ctx context.Context, operation string, req interface{}, resp interface{}) error {
	return s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.s3TablesManager.Execute(ctx, mgrClient, operation, req, resp, s3_constants.AccountAdminId)
	})
}

// S3Tables data retrieval for pages

func (s *AdminServer) GetS3TablesBucketsData(ctx context.Context) (S3TablesBucketsData, error) {
	var buckets []S3TablesBucketSummary
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          s3tables.TablesPath,
			Limit:              uint32(s3TablesAdminListLimit * 2),
			InclusiveStartFrom: true,
		})
		if err != nil {
			return err
		}
		for len(buckets) < s3TablesAdminListLimit {
			entry, recvErr := resp.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				}
				return recvErr
			}
			if entry.Entry == nil || !entry.Entry.IsDirectory {
				continue
			}
			if strings.HasPrefix(entry.Entry.Name, ".") {
				continue
			}
			if !s3tables.IsTableBucketEntry(entry.Entry) {
				continue
			}
			metaBytes, ok := entry.Entry.Extended[s3tables.ExtendedKeyMetadata]
			if !ok {
				continue
			}
			var metadata tableBucketMetadata
			if err := json.Unmarshal(metaBytes, &metadata); err != nil {
				glog.V(1).Infof("S3Tables: failed to decode table bucket metadata for %s: %v", entry.Entry.Name, err)
				continue
			}
			arn, err := s3tables.BuildBucketARN(s3tables.DefaultRegion, metadata.OwnerAccountID, entry.Entry.Name)
			if err != nil {
				glog.V(1).Infof("S3Tables: failed to build table bucket ARN for %s: %v", entry.Entry.Name, err)
				continue
			}
			buckets = append(buckets, S3TablesBucketSummary{
				ARN:            arn,
				Name:           entry.Entry.Name,
				OwnerAccountID: metadata.OwnerAccountID,
				CreatedAt:      metadata.CreatedAt,
			})
		}
		return nil
	})
	if err != nil {
		return S3TablesBucketsData{}, err
	}
	return S3TablesBucketsData{
		Buckets:      buckets,
		TotalBuckets: len(buckets),
		IcebergPort:  s.icebergPort,
		LastUpdated:  time.Now(),
	}, nil
}

func (s *AdminServer) GetS3TablesNamespacesData(ctx context.Context, bucketArn string) (S3TablesNamespacesData, error) {
	var resp s3tables.ListNamespacesResponse
	req := &s3tables.ListNamespacesRequest{TableBucketARN: bucketArn, MaxNamespaces: s3TablesAdminListLimit}
	if err := s.executeS3TablesOperation(ctx, "ListNamespaces", req, &resp); err != nil {
		return S3TablesNamespacesData{}, err
	}
	return S3TablesNamespacesData{
		BucketARN:       bucketArn,
		Namespaces:      resp.Namespaces,
		TotalNamespaces: len(resp.Namespaces),
		LastUpdated:     time.Now(),
	}, nil
}

func (s *AdminServer) GetS3TablesTablesData(ctx context.Context, bucketArn, namespace string) (S3TablesTablesData, error) {
	var resp s3tables.ListTablesResponse
	var ns []string
	if namespace != "" {
		ns = []string{namespace}
	}
	req := &s3tables.ListTablesRequest{TableBucketARN: bucketArn, Namespace: ns, MaxTables: s3TablesAdminListLimit}
	if err := s.executeS3TablesOperation(ctx, "ListTables", req, &resp); err != nil {
		return S3TablesTablesData{}, err
	}
	return S3TablesTablesData{
		BucketARN:   bucketArn,
		Namespace:   namespace,
		Tables:      resp.Tables,
		TotalTables: len(resp.Tables),
		LastUpdated: time.Now(),
	}, nil
}

// Iceberg Catalog data providers

// GetIcebergCatalogData returns the Iceberg catalog overview data.
// Each S3 Table Bucket is exposed as an Iceberg catalog.
func (s *AdminServer) GetIcebergCatalogData(ctx context.Context) (IcebergCatalogData, error) {
	bucketsData, err := s.GetS3TablesBucketsData(ctx)
	if err != nil {
		return IcebergCatalogData{}, err
	}

	catalogs := make([]IcebergCatalogInfo, 0, len(bucketsData.Buckets))
	for _, bucket := range bucketsData.Buckets {
		catalogs = append(catalogs, IcebergCatalogInfo{
			Name:           bucket.Name,
			ARN:            bucket.ARN,
			OwnerAccountID: bucket.OwnerAccountID,
			CreatedAt:      bucket.CreatedAt,
		})
	}

	return IcebergCatalogData{
		Catalogs:      catalogs,
		TotalCatalogs: len(catalogs),
		IcebergPort:   s.icebergPort, // Use the port passed to AdminServer
		LastUpdated:   time.Now(),
	}, nil
}

// GetIcebergNamespacesData returns namespaces for an Iceberg catalog.
func (s *AdminServer) GetIcebergNamespacesData(ctx context.Context, catalogName, bucketArn string) (IcebergNamespacesData, error) {
	nsData, err := s.GetS3TablesNamespacesData(ctx, bucketArn)
	if err != nil {
		return IcebergNamespacesData{}, err
	}

	namespaces := make([]IcebergNamespaceInfo, 0, len(nsData.Namespaces))
	for _, ns := range nsData.Namespaces {
		name := ""
		if len(ns.Namespace) > 0 {
			name = strings.Join(ns.Namespace, ".")
		}
		namespaces = append(namespaces, IcebergNamespaceInfo{
			Name:      name,
			CreatedAt: ns.CreatedAt,
		})
	}

	return IcebergNamespacesData{
		CatalogName:     catalogName,
		BucketARN:       bucketArn,
		Namespaces:      namespaces,
		TotalNamespaces: len(namespaces),
		LastUpdated:     time.Now(),
	}, nil
}

// GetIcebergTablesData returns tables for an Iceberg namespace.
func (s *AdminServer) GetIcebergTablesData(ctx context.Context, catalogName, bucketArn, namespace string) (IcebergTablesData, error) {
	tablesData, err := s.GetS3TablesTablesData(ctx, bucketArn, namespace)
	if err != nil {
		return IcebergTablesData{}, err
	}

	tables := make([]IcebergTableInfo, 0, len(tablesData.Tables))
	for _, t := range tablesData.Tables {
		tables = append(tables, IcebergTableInfo{
			Name:      t.Name,
			CreatedAt: t.CreatedAt,
		})
	}

	return IcebergTablesData{
		CatalogName:   catalogName,
		NamespaceName: namespace,
		BucketARN:     bucketArn,
		Tables:        tables,
		TotalTables:   len(tables),
		LastUpdated:   time.Now(),
	}, nil
}

// GetIcebergTableDetailsData returns Iceberg table metadata and snapshot information.
func (s *AdminServer) GetIcebergTableDetailsData(ctx context.Context, catalogName, bucketArn, namespace, tableName string) (IcebergTableDetailsData, error) {
	var resp s3tables.GetTableResponse
	req := &s3tables.GetTableRequest{
		TableBucketARN: bucketArn,
		Namespace:      []string{namespace},
		Name:           tableName,
	}
	if err := s.executeS3TablesOperation(ctx, "GetTable", req, &resp); err != nil {
		return IcebergTableDetailsData{}, err
	}

	details := IcebergTableDetailsData{
		CatalogName:      catalogName,
		NamespaceName:    namespace,
		TableName:        resp.Name,
		BucketARN:        bucketArn,
		TableARN:         resp.TableARN,
		Format:           resp.Format,
		CreatedAt:        resp.CreatedAt,
		ModifiedAt:       resp.ModifiedAt,
		MetadataLocation: resp.MetadataLocation,
	}

	applyIcebergMetadata(resp.Metadata, &details)
	return details, nil
}

type icebergFullMetadata struct {
	FormatVersion     int                    `json:"format-version"`
	TableUUID         string                 `json:"table-uuid"`
	Location          string                 `json:"location"`
	LastUpdatedMs     int64                  `json:"last-updated-ms"`
	Schemas           []icebergSchema        `json:"schemas"`
	Schema            *icebergSchema         `json:"schema"`
	CurrentSchemaID   int                    `json:"current-schema-id"`
	PartitionSpecs    []icebergPartitionSpec `json:"partition-specs"`
	PartitionSpec     *icebergPartitionSpec  `json:"partition-spec"`
	DefaultSpecID     int                    `json:"default-spec-id"`
	Properties        map[string]string      `json:"properties"`
	Snapshots         []icebergSnapshot      `json:"snapshots"`
	CurrentSnapshotID int64                  `json:"current-snapshot-id"`
}

type icebergSchema struct {
	SchemaID int                  `json:"schema-id"`
	Fields   []icebergSchemaField `json:"fields"`
}

type icebergSchemaField struct {
	ID       int             `json:"id"`
	Name     string          `json:"name"`
	Type     json.RawMessage `json:"type"`
	Required bool            `json:"required"`
}

type icebergPartitionSpec struct {
	SpecID int                     `json:"spec-id"`
	Fields []icebergPartitionField `json:"fields"`
}

type icebergPartitionField struct {
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id"`
	Name      string `json:"name"`
	Transform string `json:"transform"`
}

type icebergSnapshot struct {
	SnapshotID   int64             `json:"snapshot-id"`
	TimestampMs  int64             `json:"timestamp-ms"`
	ManifestList string            `json:"manifest-list"`
	Summary      map[string]string `json:"summary"`
}

func applyIcebergMetadata(metadata *s3tables.TableMetadata, details *IcebergTableDetailsData) {
	if details == nil || metadata == nil {
		return
	}
	if len(metadata.FullMetadata) == 0 {
		details.SchemaFields = schemaFieldsFromIceberg(metadata.Iceberg)
		return
	}

	var full icebergFullMetadata
	if err := json.Unmarshal(metadata.FullMetadata, &full); err != nil {
		glog.V(1).Infof("iceberg metadata parse failed: %v", err)
		details.MetadataError = fmt.Sprintf("Failed to parse Iceberg metadata: %v", err)
		details.SchemaFields = schemaFieldsFromIceberg(metadata.Iceberg)
		return
	}

	details.TableLocation = full.Location
	details.SchemaFields = schemaFieldsFromFullMetadata(full, metadata.Iceberg)
	details.PartitionFields = partitionFieldsFromFullMetadata(full)
	details.Properties = propertiesFromFullMetadata(full.Properties)
	details.Snapshots = snapshotsFromFullMetadata(full.Snapshots)
	details.SnapshotCount = len(full.Snapshots)
	details.HasSnapshotCount = true
	if metricsSnapshot := selectSnapshotForMetrics(full); metricsSnapshot != nil {
		if value, ok := parseSummaryInt(metricsSnapshot.Summary, "total-data-files", "total-data-file-count", "total-files", "total-file-count"); ok {
			details.DataFileCount = value
			details.HasDataFileCount = true
		}
		if value, ok := parseSummaryInt(metricsSnapshot.Summary, "total-files-size", "total-data-files-size", "total-file-size", "total-data-file-size", "total-data-size", "total-size"); ok {
			details.TotalSizeBytes = value
			details.HasTotalSize = true
		}
	}
}

func typeToString(t json.RawMessage) json.RawMessage {
	if t == nil || len(t) == 0 {
		return json.RawMessage(`""`)
	}
	var v interface{}
	if err := json.Unmarshal(t, &v); err != nil {
		return json.RawMessage(`"(complex)"`)
	}
	if str, ok := v.(string); ok {
		return json.RawMessage(fmt.Sprintf(`"%s"`, str))
	}
	return json.RawMessage(`"(complex)"`)
}

func schemaFieldsFromFullMetadata(full icebergFullMetadata, fallback *s3tables.IcebergMetadata) []IcebergSchemaFieldInfo {
	if schema := selectSchema(full); schema != nil {
		fields := make([]IcebergSchemaFieldInfo, 0, len(schema.Fields))
		for _, field := range schema.Fields {
			fields = append(fields, IcebergSchemaFieldInfo{
				ID:       field.ID,
				Name:     field.Name,
				Type:     typeToString(field.Type),
				Required: field.Required,
			})
		}
		return fields
	}
	return schemaFieldsFromIceberg(fallback)
}

func schemaFieldsFromIceberg(metadata *s3tables.IcebergMetadata) []IcebergSchemaFieldInfo {
	if metadata == nil {
		return nil
	}
	fields := make([]IcebergSchemaFieldInfo, 0, len(metadata.Schema.Fields))
	for _, field := range metadata.Schema.Fields {
		typeBytes, _ := json.Marshal(field.Type)
		fields = append(fields, IcebergSchemaFieldInfo{
			Name:     field.Name,
			Type:     typeBytes,
			Required: field.Required,
		})
	}
	return fields
}

func selectSchema(full icebergFullMetadata) *icebergSchema {
	if len(full.Schemas) == 0 && full.Schema == nil {
		return nil
	}
	if len(full.Schemas) == 0 {
		return full.Schema
	}
	for i := range full.Schemas {
		if full.Schemas[i].SchemaID == full.CurrentSchemaID {
			return &full.Schemas[i]
		}
	}
	return &full.Schemas[0]
}

func partitionFieldsFromFullMetadata(full icebergFullMetadata) []IcebergPartitionFieldInfo {
	var spec *icebergPartitionSpec
	if len(full.PartitionSpecs) == 0 && full.PartitionSpec == nil {
		return nil
	}
	if len(full.PartitionSpecs) == 0 {
		spec = full.PartitionSpec
	} else {
		for i := range full.PartitionSpecs {
			if full.PartitionSpecs[i].SpecID == full.DefaultSpecID {
				spec = &full.PartitionSpecs[i]
				break
			}
		}
		if spec == nil {
			spec = &full.PartitionSpecs[0]
		}
	}
	if spec == nil {
		return nil
	}
	fields := make([]IcebergPartitionFieldInfo, 0, len(spec.Fields))
	for _, field := range spec.Fields {
		fields = append(fields, IcebergPartitionFieldInfo{
			Name:      field.Name,
			Transform: field.Transform,
			SourceID:  field.SourceID,
			FieldID:   field.FieldID,
		})
	}
	return fields
}

func propertiesFromFullMetadata(properties map[string]string) []IcebergPropertyInfo {
	if len(properties) == 0 {
		return nil
	}
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entries := make([]IcebergPropertyInfo, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, IcebergPropertyInfo{Key: key, Value: properties[key]})
	}
	return entries
}

func snapshotsFromFullMetadata(snapshots []icebergSnapshot) []IcebergSnapshotInfo {
	if len(snapshots) == 0 {
		return nil
	}
	sorted := make([]icebergSnapshot, len(snapshots))
	copy(sorted, snapshots)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].TimestampMs > sorted[j].TimestampMs
	})
	info := make([]IcebergSnapshotInfo, 0, len(sorted))
	for _, snapshot := range sorted {
		operation := ""
		if snapshot.Summary != nil {
			operation = snapshot.Summary["operation"]
		}
		timestamp := time.Time{}
		if snapshot.TimestampMs > 0 {
			timestamp = time.Unix(0, snapshot.TimestampMs*int64(time.Millisecond))
		}
		info = append(info, IcebergSnapshotInfo{
			SnapshotID:   snapshot.SnapshotID,
			Timestamp:    timestamp,
			Operation:    operation,
			ManifestList: snapshot.ManifestList,
		})
	}
	return info
}

func selectSnapshotForMetrics(full icebergFullMetadata) *icebergSnapshot {
	if len(full.Snapshots) == 0 {
		return nil
	}
	for i := range full.Snapshots {
		if full.Snapshots[i].SnapshotID == full.CurrentSnapshotID {
			return &full.Snapshots[i]
		}
	}
	latest := full.Snapshots[0]
	for _, snapshot := range full.Snapshots[1:] {
		if snapshot.TimestampMs > latest.TimestampMs {
			latest = snapshot
		}
	}
	return &latest
}

func parseSummaryInt(summary map[string]string, keys ...string) (int64, bool) {
	if len(summary) == 0 {
		return 0, false
	}
	for _, key := range keys {
		value, ok := summary[key]
		if !ok || value == "" {
			continue
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			continue
		}
		return parsed, true
	}
	return 0, false
}

// API handlers

func (s *AdminServer) ListS3TablesBucketsAPI(c *gin.Context) {
	data, err := s.GetS3TablesBucketsData(c.Request.Context())
	if err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, data)
}

func (s *AdminServer) CreateS3TablesBucket(c *gin.Context) {
	var req struct {
		Name  string            `json:"name"`
		Tags  map[string]string `json:"tags"`
		Owner string            `json:"owner"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}
	if req.Name == "" {
		c.JSON(400, gin.H{"error": "Bucket name is required"})
		return
	}
	owner := strings.TrimSpace(req.Owner)
	if len(owner) > MaxOwnerNameLength {
		c.JSON(400, gin.H{"error": fmt.Sprintf("Owner name must be %d characters or less", MaxOwnerNameLength)})
		return
	}
	if len(req.Tags) > 0 {
		if err := s3tables.ValidateTags(req.Tags); err != nil {
			c.JSON(400, gin.H{"error": "Invalid tags: " + err.Error()})
			return
		}
	}
	createReq := &s3tables.CreateTableBucketRequest{Name: req.Name, Tags: req.Tags}
	var resp s3tables.CreateTableBucketResponse
	if err := s.executeS3TablesOperation(c.Request.Context(), "CreateTableBucket", createReq, &resp); err != nil {
		writeS3TablesError(c, err)
		return
	}
	if owner != "" {
		if err := s.SetTableBucketOwner(c.Request.Context(), req.Name, owner); err != nil {
			deleteReq := &s3tables.DeleteTableBucketRequest{TableBucketARN: resp.ARN}
			if deleteErr := s.executeS3TablesOperation(c.Request.Context(), "DeleteTableBucket", deleteReq, nil); deleteErr != nil {
				c.JSON(500, gin.H{"error": fmt.Sprintf("Failed to set table bucket owner: %v; rollback delete failed: %v", err, deleteErr)})
				return
			}
			writeS3TablesError(c, err)
			return
		}
	}
	c.JSON(201, gin.H{"arn": resp.ARN})
}

func (s *AdminServer) SetTableBucketOwner(ctx context.Context, bucketName, owner string) error {
	return s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
			Directory: s3tables.TablesPath,
			Name:      bucketName,
		})
		if err != nil {
			return fmt.Errorf("lookup table bucket %s: %w", bucketName, err)
		}
		if resp.Entry == nil {
			return fmt.Errorf("table bucket %s not found", bucketName)
		}
		entry := resp.Entry
		if entry.Extended == nil {
			return fmt.Errorf("table bucket %s metadata missing", bucketName)
		}
		metaBytes, ok := entry.Extended[s3tables.ExtendedKeyMetadata]
		if !ok {
			return fmt.Errorf("table bucket %s metadata missing", bucketName)
		}
		var metadata tableBucketMetadata
		if err := json.Unmarshal(metaBytes, &metadata); err != nil {
			return fmt.Errorf("failed to parse table bucket metadata: %w", err)
		}
		metadata.OwnerAccountID = owner
		updated, err := json.Marshal(&metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal table bucket metadata: %w", err)
		}
		entry.Extended[s3tables.ExtendedKeyMetadata] = updated
		if _, err := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
			Directory: s3tables.TablesPath,
			Entry:     entry,
		}); err != nil {
			return fmt.Errorf("failed to update table bucket owner: %w", err)
		}
		return nil
	})
}

func (s *AdminServer) DeleteS3TablesBucket(c *gin.Context) {
	bucketArn := c.Query("bucket")
	if bucketArn == "" {
		c.JSON(400, gin.H{"error": "Bucket ARN is required"})
		return
	}
	req := &s3tables.DeleteTableBucketRequest{TableBucketARN: bucketArn}
	if err := s.executeS3TablesOperation(c.Request.Context(), "DeleteTableBucket", req, nil); err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, gin.H{"message": "Bucket deleted"})
}

func (s *AdminServer) ListS3TablesNamespacesAPI(c *gin.Context) {
	bucketArn := c.Query("bucket")
	if bucketArn == "" {
		c.JSON(400, gin.H{"error": "bucket query parameter is required"})
		return
	}
	data, err := s.GetS3TablesNamespacesData(c.Request.Context(), bucketArn)
	if err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, data)
}

func (s *AdminServer) CreateS3TablesNamespace(c *gin.Context) {
	var req struct {
		BucketARN string `json:"bucket_arn"`
		Name      string `json:"name"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}
	if req.BucketARN == "" || req.Name == "" {
		c.JSON(400, gin.H{"error": "bucket_arn and name are required"})
		return
	}
	createReq := &s3tables.CreateNamespaceRequest{TableBucketARN: req.BucketARN, Namespace: []string{req.Name}}
	var resp s3tables.CreateNamespaceResponse
	if err := s.executeS3TablesOperation(c.Request.Context(), "CreateNamespace", createReq, &resp); err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(201, gin.H{"namespace": resp.Namespace})
}

func (s *AdminServer) DeleteS3TablesNamespace(c *gin.Context) {
	bucketArn := c.Query("bucket")
	namespace := c.Query("name")
	if bucketArn == "" || namespace == "" {
		c.JSON(400, gin.H{"error": "bucket and name query parameters are required"})
		return
	}
	req := &s3tables.DeleteNamespaceRequest{TableBucketARN: bucketArn, Namespace: []string{namespace}}
	if err := s.executeS3TablesOperation(c.Request.Context(), "DeleteNamespace", req, nil); err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, gin.H{"message": "Namespace deleted"})
}

func (s *AdminServer) ListS3TablesTablesAPI(c *gin.Context) {
	bucketArn := c.Query("bucket")
	if bucketArn == "" {
		c.JSON(400, gin.H{"error": "bucket query parameter is required"})
		return
	}
	namespace := c.Query("namespace")
	data, err := s.GetS3TablesTablesData(c.Request.Context(), bucketArn, namespace)
	if err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, data)
}

func (s *AdminServer) CreateS3TablesTable(c *gin.Context) {
	var req struct {
		BucketARN string                  `json:"bucket_arn"`
		Namespace string                  `json:"namespace"`
		Name      string                  `json:"name"`
		Format    string                  `json:"format"`
		Tags      map[string]string       `json:"tags"`
		Metadata  *s3tables.TableMetadata `json:"metadata"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}
	if req.BucketARN == "" || req.Namespace == "" || req.Name == "" {
		c.JSON(400, gin.H{"error": "bucket_arn, namespace, and name are required"})
		return
	}
	format := req.Format
	if format == "" {
		format = "ICEBERG"
	}
	if len(req.Tags) > 0 {
		if err := s3tables.ValidateTags(req.Tags); err != nil {
			c.JSON(400, gin.H{"error": "Invalid tags: " + err.Error()})
			return
		}
	}
	createReq := &s3tables.CreateTableRequest{
		TableBucketARN: req.BucketARN,
		Namespace:      []string{req.Namespace},
		Name:           req.Name,
		Format:         format,
		Tags:           req.Tags,
		Metadata:       req.Metadata,
	}
	var resp s3tables.CreateTableResponse
	if err := s.executeS3TablesOperation(c.Request.Context(), "CreateTable", createReq, &resp); err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(201, gin.H{"table_arn": resp.TableARN, "version_token": resp.VersionToken})
}

func (s *AdminServer) DeleteS3TablesTable(c *gin.Context) {
	bucketArn := c.Query("bucket")
	namespace := c.Query("namespace")
	name := c.Query("name")
	version := c.Query("version")
	if bucketArn == "" || namespace == "" || name == "" {
		c.JSON(400, gin.H{"error": "bucket, namespace, and name query parameters are required"})
		return
	}
	req := &s3tables.DeleteTableRequest{TableBucketARN: bucketArn, Namespace: []string{namespace}, Name: name, VersionToken: version}
	if err := s.executeS3TablesOperation(c.Request.Context(), "DeleteTable", req, nil); err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, gin.H{"message": "Table deleted"})
}

func (s *AdminServer) PutS3TablesBucketPolicy(c *gin.Context) {
	var req struct {
		BucketARN string `json:"bucket_arn"`
		Policy    string `json:"policy"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}
	if req.BucketARN == "" || req.Policy == "" {
		c.JSON(400, gin.H{"error": "bucket_arn and policy are required"})
		return
	}
	putReq := &s3tables.PutTableBucketPolicyRequest{TableBucketARN: req.BucketARN, ResourcePolicy: req.Policy}
	if err := s.executeS3TablesOperation(c.Request.Context(), "PutTableBucketPolicy", putReq, nil); err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, gin.H{"message": "Policy updated"})
}

func (s *AdminServer) GetS3TablesBucketPolicy(c *gin.Context) {
	bucketArn := c.Query("bucket")
	if bucketArn == "" {
		c.JSON(400, gin.H{"error": "bucket query parameter is required"})
		return
	}
	getReq := &s3tables.GetTableBucketPolicyRequest{TableBucketARN: bucketArn}
	var resp s3tables.GetTableBucketPolicyResponse
	if err := s.executeS3TablesOperation(c.Request.Context(), "GetTableBucketPolicy", getReq, &resp); err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, gin.H{"policy": resp.ResourcePolicy})
}

func (s *AdminServer) DeleteS3TablesBucketPolicy(c *gin.Context) {
	bucketArn := c.Query("bucket")
	if bucketArn == "" {
		c.JSON(400, gin.H{"error": "bucket query parameter is required"})
		return
	}
	deleteReq := &s3tables.DeleteTableBucketPolicyRequest{TableBucketARN: bucketArn}
	if err := s.executeS3TablesOperation(c.Request.Context(), "DeleteTableBucketPolicy", deleteReq, nil); err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, gin.H{"message": "Policy deleted"})
}

func (s *AdminServer) PutS3TablesTablePolicy(c *gin.Context) {
	var req struct {
		BucketARN string `json:"bucket_arn"`
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
		Policy    string `json:"policy"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}
	if req.BucketARN == "" || req.Namespace == "" || req.Name == "" || req.Policy == "" {
		c.JSON(400, gin.H{"error": "bucket_arn, namespace, name, and policy are required"})
		return
	}
	putReq := &s3tables.PutTablePolicyRequest{TableBucketARN: req.BucketARN, Namespace: []string{req.Namespace}, Name: req.Name, ResourcePolicy: req.Policy}
	if err := s.executeS3TablesOperation(c.Request.Context(), "PutTablePolicy", putReq, nil); err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, gin.H{"message": "Policy updated"})
}

func (s *AdminServer) GetS3TablesTablePolicy(c *gin.Context) {
	bucketArn := c.Query("bucket")
	namespace := c.Query("namespace")
	name := c.Query("name")
	if bucketArn == "" || namespace == "" || name == "" {
		c.JSON(400, gin.H{"error": "bucket, namespace, and name query parameters are required"})
		return
	}
	getReq := &s3tables.GetTablePolicyRequest{TableBucketARN: bucketArn, Namespace: []string{namespace}, Name: name}
	var resp s3tables.GetTablePolicyResponse
	if err := s.executeS3TablesOperation(c.Request.Context(), "GetTablePolicy", getReq, &resp); err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, gin.H{"policy": resp.ResourcePolicy})
}

func (s *AdminServer) DeleteS3TablesTablePolicy(c *gin.Context) {
	bucketArn := c.Query("bucket")
	namespace := c.Query("namespace")
	name := c.Query("name")
	if bucketArn == "" || namespace == "" || name == "" {
		c.JSON(400, gin.H{"error": "bucket, namespace, and name query parameters are required"})
		return
	}
	deleteReq := &s3tables.DeleteTablePolicyRequest{TableBucketARN: bucketArn, Namespace: []string{namespace}, Name: name}
	if err := s.executeS3TablesOperation(c.Request.Context(), "DeleteTablePolicy", deleteReq, nil); err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, gin.H{"message": "Policy deleted"})
}

func (s *AdminServer) TagS3TablesResource(c *gin.Context) {
	var req struct {
		ResourceARN string            `json:"resource_arn"`
		Tags        map[string]string `json:"tags"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}
	if req.ResourceARN == "" || len(req.Tags) == 0 {
		c.JSON(400, gin.H{"error": "resource_arn and tags are required"})
		return
	}
	if err := s3tables.ValidateTags(req.Tags); err != nil {
		c.JSON(400, gin.H{"error": "Invalid tags: " + err.Error()})
		return
	}
	tagReq := &s3tables.TagResourceRequest{ResourceARN: req.ResourceARN, Tags: req.Tags}
	if err := s.executeS3TablesOperation(c.Request.Context(), "TagResource", tagReq, nil); err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, gin.H{"message": "Tags updated"})
}

func (s *AdminServer) ListS3TablesTags(c *gin.Context) {
	resourceArn := c.Query("arn")
	if resourceArn == "" {
		c.JSON(400, gin.H{"error": "arn query parameter is required"})
		return
	}
	listReq := &s3tables.ListTagsForResourceRequest{ResourceARN: resourceArn}
	var resp s3tables.ListTagsForResourceResponse
	if err := s.executeS3TablesOperation(c.Request.Context(), "ListTagsForResource", listReq, &resp); err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, resp)
}

func (s *AdminServer) UntagS3TablesResource(c *gin.Context) {
	var req struct {
		ResourceARN string   `json:"resource_arn"`
		TagKeys     []string `json:"tag_keys"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}
	if req.ResourceARN == "" || len(req.TagKeys) == 0 {
		c.JSON(400, gin.H{"error": "resource_arn and tag_keys are required"})
		return
	}
	untagReq := &s3tables.UntagResourceRequest{ResourceARN: req.ResourceARN, TagKeys: req.TagKeys}
	if err := s.executeS3TablesOperation(c.Request.Context(), "UntagResource", untagReq, nil); err != nil {
		writeS3TablesError(c, err)
		return
	}
	c.JSON(200, gin.H{"message": "Tags removed"})
}

func parseS3TablesErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	var s3Err *s3tables.S3TablesError
	if errors.As(err, &s3Err) {
		if s3Err.Message != "" {
			return fmt.Sprintf("%s: %s", s3Err.Type, s3Err.Message)
		}
		return s3Err.Type
	}
	return err.Error()
}

func writeS3TablesError(c *gin.Context, err error) {
	c.JSON(s3TablesErrorStatus(err), gin.H{"error": parseS3TablesErrorMessage(err)})
}

func s3TablesErrorStatus(err error) int {
	var s3Err *s3tables.S3TablesError
	if errors.As(err, &s3Err) {
		switch s3Err.Type {
		case s3tables.ErrCodeInvalidRequest:
			return http.StatusBadRequest
		case s3tables.ErrCodeNoSuchBucket, s3tables.ErrCodeNoSuchNamespace, s3tables.ErrCodeNoSuchTable, s3tables.ErrCodeNoSuchPolicy:
			return http.StatusNotFound
		case s3tables.ErrCodeAccessDenied:
			return http.StatusForbidden
		case s3tables.ErrCodeBucketAlreadyExists, s3tables.ErrCodeNamespaceAlreadyExists, s3tables.ErrCodeTableAlreadyExists, s3tables.ErrCodeConflict:
			return http.StatusConflict
		}
	}
	return http.StatusInternalServerError
}
