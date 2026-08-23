package lance

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

const defaultPageSize = 1000

// execute runs one S3 Tables operation as the request's authenticated caller.
func (s *Server) execute(r *http.Request, operation string, req, resp interface{}) error {
	identityName := s3_constants.GetIdentityNameFromContext(r)
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return s.tablesManager.Execute(r.Context(), s3tables.NewManagerClient(client), operation, req, resp, identityName)
	})
}

func bucketARN(bucket string) string {
	arn, _ := s3tables.BuildBucketARN(s3tables.DefaultRegion, s3_constants.AccountAdminId, bucket)
	return arn
}

// maxRequestBody bounds what one call can make the catalog hold. Every request
// this surface takes is a small JSON envelope; the largest carries a set of
// properties, not data.
const maxRequestBody = 4 << 20

// decodeBody reads an optional JSON request body. Every Lance operation carries
// one, but the fields that matter are also in the route, so an empty body is
// not an error.
func decodeBody(r *http.Request, into interface{}) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil {
		return fmt.Errorf("read request body: %w", err)
	}
	if len(body) > maxRequestBody {
		return fmt.Errorf("request body is larger than %d bytes", maxRequestBody)
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, into); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	return nil
}

// routeIdentifier parses the {id} route variable, writing the error response
// itself when the identifier is malformed.
func routeIdentifier(w http.ResponseWriter, r *http.Request) (identifier, string, bool) {
	delimiter := requestDelimiter(r)
	id, err := parseIdentifier(mux.Vars(r)["id"], delimiter)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidInput, err.Error())
		return nil, delimiter, false
	}
	return id, delimiter, true
}

// checkBodyIdentifier enforces the spec rule that a route and a body naming
// different objects is a bad request rather than a silent preference.
func checkBodyIdentifier(w http.ResponseWriter, r *http.Request, id identifier, body []string) bool {
	if id.matchesBody(body) {
		return true
	}
	writeError(w, r, http.StatusBadRequest, codeInvalidInput,
		"the identifier in the request body does not match the one in the route")
	return false
}

func pageSize(r *http.Request) int {
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			return parsed
		}
	}
	return defaultPageSize
}

func boolQuery(r *http.Request, name string) bool {
	value, err := strconv.ParseBool(r.URL.Query().Get(name))
	return err == nil && value
}

// wants resolves a tri-state request flag: the query parameter the REST spec
// adds, else the body field, else the implementation's own choice.
func wants(r *http.Request, name string, body *bool) bool {
	if boolQuery(r, name) {
		return true
	}
	return body != nil && *body
}

// tableLocation is where a table's dataset lives when the caller does not name
// a location. It mirrors the Iceberg catalog's layout so both catalogs put a
// table of the same name in the same place.
func tableLocation(bucket string, ns []string, name string) string {
	return fmt.Sprintf("s3://%s/%s/%s", bucket, strings.Join(ns, "."), name)
}

// confineLocation rejects a client-supplied table location that does not resolve
// inside the caller's own bucket, the way the Iceberg gateway confines the same
// field. The location feeds TableDataDirFromMetadataLocation, which joins it
// under /buckets and collapses any "../" segments; an unconfined location lets a
// marker write escape into another tenant's namespace.
func confineLocation(bucket, location string) error {
	rest, ok := strings.CutPrefix(location, "s3://")
	if !ok {
		return fmt.Errorf("location must be an s3:// URI")
	}
	name, keys, _ := strings.Cut(rest, "/")
	if name != bucket {
		return fmt.Errorf("location must be within bucket %s", bucket)
	}
	for _, segment := range strings.Split(keys, "/") {
		switch {
		case segment == "." || segment == "..":
			return fmt.Errorf("location must not contain a path traversal segment")
		case strings.ContainsAny(segment, "\\\x00"):
			return fmt.Errorf("location contains an invalid character")
		}
	}
	return nil
}

// storageOptions builds the object_store settings a Lance client needs to reach
// the dataset. The key names are the aws_-prefixed forms Lance clients pass
// through to object_store.
func (s *Server) storageOptions(r *http.Request, bucket, location string, vend bool) (map[string]string, error) {
	options := map[string]string{}
	if s.s3Endpoint != "" {
		options["aws_endpoint"] = s.s3Endpoint
		if strings.HasPrefix(s.s3Endpoint, "http://") {
			// object_store refuses a plaintext endpoint unless told to allow it,
			// and the resulting failure reads like a credential problem.
			options["allow_http"] = "true"
		}
	}
	if s.s3Region != "" {
		options["aws_region"] = s.s3Region
	}

	if !vend || s.credentialVendor == nil {
		return options, nil
	}

	principal := s3_constants.GetIdentityNameFromContext(r)
	credentials, err := s.credentialVendor.VendTableCredentials(r.Context(), principal, bucket, locationPrefix(location))
	if err != nil {
		return nil, err
	}
	if credentials == nil {
		return options, nil
	}
	options["aws_access_key_id"] = credentials.AccessKeyID
	options["aws_secret_access_key"] = credentials.SecretAccessKey
	if credentials.SessionToken != "" {
		options["aws_session_token"] = credentials.SessionToken
	}
	if !credentials.Expiration.IsZero() {
		options["expires_at_millis"] = strconv.FormatInt(credentials.Expiration.UnixMilli(), 10)
	}
	return options, nil
}

// locationPrefix strips the s3://bucket/ part of a location, leaving the key
// prefix a credential is scoped to.
func locationPrefix(location string) string {
	trimmed := strings.TrimPrefix(location, "s3://")
	if _, prefix, found := strings.Cut(trimmed, "/"); found {
		return prefix
	}
	return ""
}
