package s3tables

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// Manager provides reusable S3 Tables operations for shell/admin without HTTP routing.
type Manager struct {
	handler *S3TablesHandler
}

// NewManager creates a new Manager.
func NewManager() *Manager {
	m := &Manager{handler: NewS3TablesHandler()}
	// Default to allowing access when IAM is not configured
	m.handler.SetDefaultAllow(true)
	return m
}

// SetRegion sets the AWS region for ARN generation.
func (m *Manager) SetRegion(region string) {
	m.handler.SetRegion(region)
}

// SetAccountID sets the AWS account ID for ARN generation.
func (m *Manager) SetAccountID(accountID string) {
	m.handler.SetAccountID(accountID)
}

// SetDefaultAllow sets whether to allow access by default.
func (m *Manager) SetDefaultAllow(allow bool) {
	m.handler.SetDefaultAllow(allow)
}

// SetTrusted lets trusted local tooling (shell, admin console) bypass authorization.
func (m *Manager) SetTrusted(trusted bool) {
	m.handler.SetTrusted(trusted)
}

// PrincipalFor resolves the caller of r the way the catalog's handlers do,
// so it matches the owner CreateTableBucket records. A nil request is the
// handler's account ID.
func (m *Manager) PrincipalFor(r *http.Request) string {
	if r == nil {
		return m.handler.accountID
	}
	return m.handler.getAccountID(r)
}

// PolicyContext builds the evaluation context a Manager call's synthesized
// request produces: the configured local-account predicate, the caller's own
// identity actions so admin and action grants resolve on privileges rather
// than the forwarded name, plus the open-by-default fallback only for a
// trusted caller or no resolved identity.
func (m *Manager) PolicyContext(r *http.Request) *PolicyContext {
	ctx := &PolicyContext{IsLocalAccountID: m.handler.localAccountIDs, IdentityActions: getIdentityActions(r),
		TrustedIssuers: m.handler.trustedIssuers(r)}
	if m.handler.trusted || (m.handler.defaultAllow && s3_constants.GetIdentityNameFromContext(r) == "") {
		ctx.DefaultAllow = true
	}
	return ctx
}

// Execute runs an S3 Tables operation and decodes the response into resp (if provided).
func (m *Manager) Execute(ctx context.Context, filerClient FilerClient, operation string, req interface{}, resp interface{}, identity string) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := newManagerRequest(ctx, operation, body, identity)
	if err != nil {
		return err
	}
	recorder := httptest.NewRecorder()
	m.handler.HandleRequest(recorder, httpReq, filerClient)
	return decodeS3TablesHTTPResponse(recorder, resp)
}

// AuthorizeCreateTable checks that identity may create the table the request
// describes, without creating anything. A deferred create (Iceberg
// stage-create) writes into the table bucket long before it registers the
// table, so it passes this gate first.
func (m *Manager) AuthorizeCreateTable(ctx context.Context, filerClient FilerClient, req *CreateTableRequest, identity string) error {
	httpReq, err := newManagerRequest(ctx, "CreateTable", nil, identity)
	if err != nil {
		return err
	}
	recorder := httptest.NewRecorder()
	target, authErr := m.handler.authorizeCreateTable(recorder, httpReq, filerClient, req.TableBucketARN, req.Namespace, req.Name, req.Tags, req.AllowMissingNamespace)
	if authErr == nil {
		return m.handler.checkVectorNamespaceWrite(ctx, filerClient, target.bucketName, req.Namespace)
	}
	if decoded := decodeS3TablesHTTPResponse(recorder, nil); decoded != nil {
		return decoded
	}
	return authErr
}

// AuthorizeUpdateTable checks that identity may update the table the request
// describes, without updating anything. Credential vending gates on the same
// permission a commit needs.
func (m *Manager) AuthorizeUpdateTable(ctx context.Context, filerClient FilerClient, req *UpdateTableRequest, identity string) error {
	httpReq, err := newManagerRequest(ctx, "UpdateTable", nil, identity)
	if err != nil {
		return err
	}
	recorder := httptest.NewRecorder()
	_, authErr := m.handler.authorizeUpdateTable(recorder, httpReq, filerClient, req)
	if authErr == nil {
		return nil
	}
	if decoded := decodeS3TablesHTTPResponse(recorder, nil); decoded != nil {
		return decoded
	}
	return authErr
}

// AuthorizeUpdateView checks that identity may update the view the request
// describes, without updating anything.
func (m *Manager) AuthorizeUpdateView(ctx context.Context, filerClient FilerClient, req *UpdateViewRequest, identity string) error {
	httpReq, err := newManagerRequest(ctx, "UpdateView", nil, identity)
	if err != nil {
		return err
	}
	recorder := httptest.NewRecorder()
	_, authErr := m.handler.authorizeUpdateView(recorder, httpReq, filerClient, req)
	if authErr == nil {
		return nil
	}
	if decoded := decodeS3TablesHTTPResponse(recorder, nil); decoded != nil {
		return decoded
	}
	return authErr
}

func newManagerRequest(ctx context.Context, operation string, body []byte, identity string) (*http.Request, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "/", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/x-amz-json-1.1")
	httpReq.Header.Set("X-Amz-Target", "S3Tables."+operation)
	if identity != "" {
		httpReq.Header.Set(s3_constants.AmzAccountId, identity)
		httpReq = httpReq.WithContext(s3_constants.SetIdentityNameInContext(httpReq.Context(), identity))
	}
	return httpReq, nil
}

func decodeS3TablesHTTPResponse(recorder *httptest.ResponseRecorder, resp interface{}) error {
	result := recorder.Result()
	defer result.Body.Close()
	data, err := io.ReadAll(result.Body)
	if err != nil {
		return err
	}
	if result.StatusCode >= http.StatusBadRequest {
		var errResp S3TablesError
		if len(data) > 0 {
			if jsonErr := json.Unmarshal(data, &errResp); jsonErr == nil && (errResp.Type != "" || errResp.Message != "") {
				return &errResp
			}
		}
		return &S3TablesError{Type: ErrCodeInternalError, Message: string(bytes.TrimSpace(data))}
	}
	if resp == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, resp); err != nil {
		return err
	}
	return nil
}

// ManagerClient adapts a SeaweedFilerClient to the FilerClient interface.
type ManagerClient struct {
	client filer_pb.SeaweedFilerClient
}

// NewManagerClient wraps a filer client.
func NewManagerClient(client filer_pb.SeaweedFilerClient) *ManagerClient {
	return &ManagerClient{client: client}
}

// WithFilerClient implements FilerClient.
func (m *ManagerClient) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	if m.client == nil {
		return errors.New("nil filer client")
	}
	return fn(m.client)
}
