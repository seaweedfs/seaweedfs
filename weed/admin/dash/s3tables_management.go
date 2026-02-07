package dash

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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
		Tables:        tables,
		TotalTables:   len(tables),
		LastUpdated:   time.Now(),
	}, nil
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
