package s3vectors

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// maxResourceTags is how many tags a vector bucket or index may carry.
const maxResourceTags = 50

const tagsPathPrefix = "/tags/"

// tagOperation names the tagging call a /tags/{resourceArn} request makes.
func tagOperation(method string) string {
	switch method {
	case http.MethodGet:
		return "ListTagsForResource"
	case http.MethodDelete:
		return "UntagResource"
	}
	return "TagResource"
}

// readTagRequest decodes the body of a POST; GET and DELETE carry everything
// in the path and query.
func readTagRequest(r *http.Request, req interface{}) error {
	if r.Method != http.MethodPost {
		return nil
	}
	return readJSON(r, req)
}

// tagTarget resolves a tagging call's resourceArn, taken from the path when
// the request was routed there, to the vector bucket or index and the ARN the
// catalog keys that record by.
func (s *Server) tagTarget(r *http.Request, bodyArn string) (vectorBucketARNs, string, error) {
	arn := bodyArn
	if v := mux.Vars(r)["resourceArn"]; v != "" {
		arn = v
	}
	var v vectorBucketARNs
	if arn == "" {
		return v, "", fmt.Errorf("resourceArn is required")
	}
	if !strings.HasPrefix(arn, "arn:") {
		return v, "", fmt.Errorf("resourceArn must be an ARN, got %q", arn)
	}
	if err := s.arnParts(&v, "", arn); err != nil {
		return v, "", err
	}
	catalogArn := s.s3tablesArn(v.bucket)
	if v.index != "" {
		catalogArn = fmt.Sprintf("%s/table/%s/%s", catalogArn, indexNamespace, v.index)
	}
	return v, catalogArn, nil
}

func (s *Server) resourceTags(r *http.Request, catalogArn string) (map[string]string, error) {
	var resp s3tables.ListTagsForResourceResponse
	if err := s.internalCatalog(r.Context(), "ListTagsForResource",
		&s3tables.ListTagsForResourceRequest{ResourceARN: catalogArn}, &resp); err != nil {
		return nil, err
	}
	if resp.Tags == nil {
		resp.Tags = map[string]string{}
	}
	return resp.Tags, nil
}

func (s *Server) handleTagResource(w http.ResponseWriter, r *http.Request) {
	var req TagResourceRequest
	if err := readTagRequest(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, catalogArn, err := s.tagTarget(r, req.ResourceArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if len(req.Tags) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "tags are required")
		return
	}
	if err := s3tables.ValidateTagsWithLimit(req.Tags, maxResourceTags); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if _, err := s.authorizeVector(r.Context(), r, "s3vectors:TagResource", arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	current, err := s.resourceTags(r, catalogArn)
	if err != nil {
		catalogErr(w, err)
		return
	}
	count := len(current)
	for k := range req.Tags {
		if _, ok := current[k]; !ok {
			count++
		}
	}
	if count > maxResourceTags {
		writeError(w, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("a resource can have at most %d tags, this request would leave %d", maxResourceTags, count))
		return
	}
	if err := s.internalCatalog(r.Context(), "TagResource",
		&s3tables.TagResourceRequest{ResourceARN: catalogArn, Tags: req.Tags}, nil); err != nil {
		catalogErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &TagResourceResponse{})
}

func (s *Server) handleUntagResource(w http.ResponseWriter, r *http.Request) {
	var req UntagResourceRequest
	if err := readTagRequest(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if keys, ok := r.URL.Query()["tagKeys"]; ok {
		req.TagKeys = keys
	}
	arns, catalogArn, err := s.tagTarget(r, req.ResourceArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if len(req.TagKeys) == 0 {
		writeError(w, http.StatusBadRequest, "ValidationException", "tagKeys are required")
		return
	}
	if _, err := s.authorizeVector(r.Context(), r, "s3vectors:UntagResource", arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	if err := s.internalCatalog(r.Context(), "UntagResource",
		&s3tables.UntagResourceRequest{ResourceARN: catalogArn, TagKeys: req.TagKeys}, nil); err != nil {
		catalogErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &UntagResourceResponse{})
}

func (s *Server) handleListTagsForResource(w http.ResponseWriter, r *http.Request) {
	var req ListTagsForResourceRequest
	if err := readTagRequest(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	arns, catalogArn, err := s.tagTarget(r, req.ResourceArn)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}
	if _, err := s.authorizeVector(r.Context(), r, "s3vectors:ListTagsForResource", arns.bucket, arns.index); err != nil {
		catalogErr(w, err)
		return
	}
	tags, err := s.resourceTags(r, catalogArn)
	if err != nil {
		catalogErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, &ListTagsForResourceResponse{Tags: tags})
}
