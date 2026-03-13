package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// exportRequest is the JSON body for POST /export.
type exportRequest struct {
	Bucket      string `json:"bucket"`
	KeyPrefix   string `json:"key_prefix"`
	S3Endpoint  string `json:"s3_endpoint"`
	S3AccessKey string `json:"s3_access_key"`
	S3SecretKey string `json:"s3_secret_key"`
	S3Region    string `json:"s3_region"`
	SnapshotID  uint32 `json:"snapshot_id"` // 0 = auto-create temp snapshot
}

// importRequest is the JSON body for POST /import.
type importRequest struct {
	Bucket         string `json:"bucket"`
	ManifestKey    string `json:"manifest_key"`
	S3Endpoint     string `json:"s3_endpoint"`
	S3AccessKey    string `json:"s3_access_key"`
	S3SecretKey    string `json:"s3_secret_key"`
	S3Region       string `json:"s3_region"`
	AllowOverwrite bool   `json:"allow_overwrite"`
}

// exportResponse is the JSON response for POST /export.
type exportResponse struct {
	OK          bool   `json:"ok"`
	ManifestKey string `json:"manifest_key"`
	DataKey     string `json:"data_key"`
	SizeBytes   uint64 `json:"size_bytes"`
	SHA256      string `json:"sha256"`
}

// importResponse is the JSON response for POST /import.
type importResponse struct {
	OK          bool   `json:"ok"`
	SizeBytes   uint64 `json:"size_bytes"`
	SHA256      string `json:"sha256"`
}

func newS3Session(endpoint, accessKey, secretKey, region string) (*session.Session, error) {
	if region == "" {
		region = "us-east-1"
	}
	cfg := &aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(true),
	}
	if accessKey != "" && secretKey != "" {
		cfg.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, "")
	} else {
		cfg.Credentials = credentials.AnonymousCredentials
	}
	return session.NewSession(cfg)
}

func (a *adminServer) handleExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	var req exportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Bucket == "" || req.S3Endpoint == "" {
		jsonError(w, "bucket and s3_endpoint are required", http.StatusBadRequest)
		return
	}

	sess, err := newS3Session(req.S3Endpoint, req.S3AccessKey, req.S3SecretKey, req.S3Region)
	if err != nil {
		jsonError(w, "s3 session: "+err.Error(), http.StatusInternalServerError)
		return
	}

	dataKey := req.KeyPrefix + "data.raw"
	manifestKey := req.KeyPrefix + "manifest.json"

	// Pipe: export writes → S3 uploader reads.
	pr, pw := io.Pipe()
	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = 8 * 1024 * 1024 // 8MB parts
		u.Concurrency = 1
	})

	var manifest *blockvol.SnapshotArtifactManifest
	var exportErr error
	exportDone := make(chan struct{})

	go func() {
		defer close(exportDone)
		defer pw.Close()
		manifest, exportErr = a.vol.ExportSnapshot(r.Context(), pw, blockvol.ExportOptions{
			DataObjectKey: dataKey,
			SnapshotID:    req.SnapshotID,
		})
		if exportErr != nil {
			pw.CloseWithError(exportErr)
		}
	}()

	// Upload data object.
	_, uploadErr := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(dataKey),
		Body:   pr,
	})
	<-exportDone

	if exportErr != nil {
		jsonError(w, "export: "+exportErr.Error(), http.StatusInternalServerError)
		return
	}
	if uploadErr != nil {
		jsonError(w, "s3 upload data: "+uploadErr.Error(), http.StatusInternalServerError)
		return
	}

	// Upload manifest.
	manifestData, err := blockvol.MarshalManifest(manifest)
	if err != nil {
		jsonError(w, "marshal manifest: "+err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(manifestKey),
		Body:   bytes.NewReader(manifestData),
	})
	if err != nil {
		jsonError(w, "s3 upload manifest: "+err.Error(), http.StatusInternalServerError)
		return
	}

	a.logger.Printf("admin: exported snapshot to s3://%s/%s (%d bytes, sha256=%s)",
		req.Bucket, dataKey, manifest.DataSizeBytes, manifest.SHA256)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(exportResponse{
		OK:          true,
		ManifestKey: manifestKey,
		DataKey:     dataKey,
		SizeBytes:   manifest.DataSizeBytes,
		SHA256:      manifest.SHA256,
	})
}

func (a *adminServer) handleImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	var req importRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Bucket == "" || req.ManifestKey == "" || req.S3Endpoint == "" {
		jsonError(w, "bucket, manifest_key, and s3_endpoint are required", http.StatusBadRequest)
		return
	}

	sess, err := newS3Session(req.S3Endpoint, req.S3AccessKey, req.S3SecretKey, req.S3Region)
	if err != nil {
		jsonError(w, "s3 session: "+err.Error(), http.StatusInternalServerError)
		return
	}
	s3Client := s3.New(sess)

	// Download manifest.
	manifestResp, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.ManifestKey),
	})
	if err != nil {
		jsonError(w, "s3 get manifest: "+err.Error(), http.StatusInternalServerError)
		return
	}
	manifestData, err := io.ReadAll(manifestResp.Body)
	manifestResp.Body.Close()
	if err != nil {
		jsonError(w, "read manifest: "+err.Error(), http.StatusInternalServerError)
		return
	}

	manifest, err := blockvol.UnmarshalManifest(manifestData)
	if err != nil {
		jsonError(w, "invalid manifest: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Download data object.
	dataResp, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(manifest.DataObjectKey),
	})
	if err != nil {
		jsonError(w, "s3 get data: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer dataResp.Body.Close()

	// Import. WARNING: import is destructive and non-atomic. If it fails mid-stream
	// (network error, context cancellation), the volume has mixed old+new data and
	// should be discarded or re-imported with allow_overwrite=true.
	err = a.vol.ImportSnapshot(r.Context(), manifest, dataResp.Body, blockvol.ImportOptions{
		AllowOverwrite: req.AllowOverwrite,
	})
	if err != nil {
		jsonError(w, "import: "+err.Error(), http.StatusConflict)
		return
	}

	a.logger.Printf("admin: imported snapshot from s3://%s/%s (%d bytes, sha256=%s)",
		req.Bucket, req.ManifestKey, manifest.DataSizeBytes, manifest.SHA256)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(importResponse{
		OK:        true,
		SizeBytes: manifest.DataSizeBytes,
		SHA256:    manifest.SHA256,
	})
}

// Ensure log is used (for future debugging).
var _ = log.Printf
