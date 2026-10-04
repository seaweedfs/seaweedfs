package s3api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/require"
)

// objectACLUpdateFiler 记录处理器实际写入的ACL，验证权限拒绝不会修改对象。
type objectACLUpdateFiler struct {
	fakeLookupFiler
	mu     sync.Mutex
	update *filer_pb.UpdateEntryRequest
}

// UpdateEntry 保存实际RPC请求，供测试核对目标路径、拥有者与授权列表。
func (f *objectACLUpdateFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.update = req
	return &filer_pb.UpdateEntryResponse{}, nil
}

// TestPutObjectAclPermissions 验证签名请求的策略和范围授权，以及历史拥有者的ACL授权。
func TestPutObjectAclPermissions(t *testing.T) {
	const bucket, object, owner = "acl-bucket", "allowed/image.png", "object-owner"
	const bucketOwner = "bucket-owner"
	xmlACL := func(ownerID, granteeID string) string {
		return fmt.Sprintf(`<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>%s</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>%s</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>`, ownerID, granteeID)
	}
	tests := []struct {
		name          string
		account       string
		action        Action
		acl           string
		policy        string
		retiredOwner  bool
		ownerMetadata string
		body          string
		status        int
	}{
		{name: "桶内授权", account: owner, action: "WriteAcp:acl-bucket", acl: "private", status: http.StatusOK},
		{name: "前缀内授权", account: owner, action: "WriteAcp:acl-bucket/allowed/*", acl: "public-read", status: http.StatusOK},
		{name: "前缀外拒绝", account: owner, action: "WriteAcp:acl-bucket/other/*", acl: "private", status: http.StatusForbidden},
		{name: "其他桶拒绝", account: owner, action: "WriteAcp:other-bucket", acl: "private", status: http.StatusForbidden},
		{name: "只读权限拒绝", account: owner, action: "Read:acl-bucket", acl: "private", status: http.StatusForbidden},
		{name: "非拥有者拒绝", account: "other-owner", action: "WriteAcp:acl-bucket", acl: "private", status: http.StatusForbidden},
		{name: "管理员设置私有", account: AccountAdmin.Id, action: "Admin", acl: "private", status: http.StatusOK},
		{name: "管理员设置公共读", account: AccountAdmin.Id, action: "Admin", acl: "public-read", status: http.StatusOK},
		{name: "IAM策略授权", account: owner, policy: "iam-allow", acl: "private", status: http.StatusOK},
		{name: "桶策略授权", account: owner, policy: "bucket-allow", acl: "private", status: http.StatusOK},
		{name: "IAM显式拒绝", account: owner, action: "WriteAcp:acl-bucket", policy: "iam-deny", acl: "private", status: http.StatusForbidden},
		{name: "桶策略显式拒绝", account: owner, action: "WriteAcp:acl-bucket", policy: "bucket-deny", acl: "private", status: http.StatusForbidden},
		{name: "历史拥有者私有", account: AccountAdmin.Id, action: "Admin", acl: "private", retiredOwner: true, status: http.StatusOK},
		{name: "历史拥有者公共读", account: AccountAdmin.Id, action: "Admin", acl: "public-read", retiredOwner: true, status: http.StatusOK},
		{name: "不同桶拥有者读权限", account: AccountAdmin.Id, action: "Admin", acl: "bucket-owner-read", status: http.StatusOK},
		{name: "不同桶拥有者完全控制", account: AccountAdmin.Id, action: "Admin", acl: "bucket-owner-full-control", status: http.StatusOK},
		{name: "XML保留历史拥有者", account: AccountAdmin.Id, action: "Admin", body: xmlACL(owner, owner), retiredOwner: true, status: http.StatusOK},
		{name: "XML未知授权对象拒绝", account: AccountAdmin.Id, action: "Admin", body: xmlACL(owner, "unknown-grantee"), retiredOwner: true, status: http.StatusBadRequest},
		{name: "XML更换拥有者拒绝", account: AccountAdmin.Id, action: "Admin", body: xmlACL("other-owner", owner), status: http.StatusForbidden},
		{name: "无扩展元数据拒绝", account: owner, action: "WriteAcp:acl-bucket", acl: "private", ownerMetadata: "nil", status: http.StatusForbidden},
		{name: "缺少拥有者拒绝", account: owner, action: "WriteAcp:acl-bucket", acl: "private", ownerMetadata: "absent", status: http.StatusForbidden},
		{name: "空拥有者拒绝", account: owner, action: "WriteAcp:acl-bucket", acl: "private", ownerMetadata: "empty", status: http.StatusForbidden},
		{name: "无拥有者IAM策略拒绝", account: owner, policy: "iam-allow", acl: "private", ownerMetadata: "absent", status: http.StatusForbidden},
		{name: "无拥有者桶策略拒绝", account: owner, policy: "bucket-allow", acl: "private", ownerMetadata: "absent", status: http.StatusForbidden},
		{name: "管理员接管无拥有者私有", account: AccountAdmin.Id, action: "Admin", acl: "private", ownerMetadata: "absent", status: http.StatusOK},
		{name: "管理员接管无拥有者公共读", account: AccountAdmin.Id, action: "Admin", acl: "public-read", ownerMetadata: "nil", status: http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filer := &objectACLUpdateFiler{fakeLookupFiler: fakeLookupFiler{entry: &filer_pb.Entry{
				Name: "image.png", Extended: map[string][]byte{s3_constants.ExtAmzOwnerKey: []byte(owner)},
			}}}
			// 模拟Filer直接写入或历史对象的不同缺失形式，不能将请求账号当作真实拥有者。
			switch tt.ownerMetadata {
			case "nil":
				filer.entry.Extended = nil
			case "absent":
				delete(filer.entry.Extended, s3_constants.ExtAmzOwnerKey)
			case "empty":
				filer.entry.Extended[s3_constants.ExtAmzOwnerKey] = []byte{}
			}
			s3a := newHeadBucketTestServer(t, filer)
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			s3a.bucketConfigCache.Set(bucket, &BucketConfig{Name: bucket, Ownership: s3_constants.OwnershipObjectWriter, Owner: bucketOwner})
			s3a.iam = NewIdentityAccessManagementWithStore(s3a.option, nil, "memory")
			t.Cleanup(s3a.iam.Shutdown)
			s3a.iam.isAuthEnabled = true
			account := &Account{Id: tt.account, DisplayName: tt.account}
			identity := &Identity{
				Name: "acl-test-user", Account: account, Actions: []Action{tt.action}, IsStatic: true,
				Credentials: []*Credential{{AccessKey: routingTestAccessKey, SecretKey: routingTestSecretKey}},
			}
			if tt.action == "" {
				identity.Actions = nil
			}
			s3a.iam.accessKeyIdent[routingTestAccessKey] = identity
			s3a.iam.nameToIdentity[identity.Name] = identity
			if !tt.retiredOwner {
				s3a.iam.accounts[owner] = &Account{Id: owner, DisplayName: owner}
			}
			s3a.iam.accounts[bucketOwner] = &Account{Id: bucketOwner, DisplayName: bucketOwner}
			s3a.iam.accounts[account.Id] = account
			if tt.policy != "" {
				effect := "Allow"
				if strings.HasSuffix(tt.policy, "deny") {
					effect = "Deny"
				}
				statement := fmt.Sprintf(`{"Effect":%q,"Action":"s3:PutObjectAcl","Resource":"arn:aws:s3:::acl-bucket/allowed/*"}`, effect)
				if strings.HasPrefix(tt.policy, "iam") {
					require.NoError(t, s3a.iam.PutPolicy("acl-policy", `{"Version":"2012-10-17","Statement":[`+statement+`]}`))
					identity.PolicyNames = []string{"acl-policy"}
				} else {
					statement = strings.Replace(statement, `{"Effect":`, `{"Principal":"*","Effect":`, 1)
					s3a.iam.policyEngine = NewBucketPolicyEngine()
					require.NoError(t, s3a.iam.policyEngine.engine.SetBucketPolicy(bucket, `{"Version":"2012-10-17","Statement":[`+statement+`]}`))
				}
			}

			req := httptest.NewRequest(http.MethodPut, "http://s3/"+bucket+"/"+object+"?acl", strings.NewReader(tt.body))
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": object})
			req.Header.Set(s3_constants.AmzCannedAcl, tt.acl)
			signRoutingTestRequest(t, req, tt.body, "s3")
			// 签名助手会替换正文；还原真实HTTP服务端的无正文请求，避免被当成空XML。
			if tt.body == "" {
				req.Body = http.NoBody
			}
			rr := httptest.NewRecorder()
			s3a.iam.Auth(s3a.PutObjectAclHandler, s3_constants.ACTION_WRITE_ACP)(rr, req)
			require.Equal(t, tt.status, rr.Code, rr.Body.String())

			filer.mu.Lock()
			update := filer.update
			filer.mu.Unlock()
			if tt.status != http.StatusOK {
				require.Nil(t, update, "拒绝的请求不得写入元数据")
				return
			}
			require.NotNil(t, update)
			require.Equal(t, "/buckets/acl-bucket/allowed", update.Directory)
			wantOwner := owner
			if tt.ownerMetadata != "" {
				// 管理员继续按既有规则为无拥有者对象建立拥有者和完全控制授权。
				wantOwner = tt.account
			}
			require.Equal(t, wantOwner, string(update.Entry.Extended[s3_constants.ExtAmzOwnerKey]))
			grants := GetAcpGrants(update.Entry.Extended)
			wantGrants := 1
			if tt.acl == "public-read" || strings.HasPrefix(tt.acl, "bucket-owner-") {
				wantGrants = 2
			}
			require.Len(t, grants, wantGrants)
			require.Equal(t, wantOwner, aws.StringValue(grants[0].Grantee.ID), "完全控制授权必须与对象拥有者一致")
			require.Equal(t, s3_constants.PermissionFullControl, aws.StringValue(grants[0].Permission))
			if tt.acl == "public-read" {
				require.Equal(t, s3_constants.GranteeGroupAllUsers, aws.StringValue(grants[1].Grantee.URI))
				require.Equal(t, s3_constants.PermissionRead, aws.StringValue(grants[1].Permission))
			}
			if strings.HasPrefix(tt.acl, "bucket-owner-") {
				require.Equal(t, bucketOwner, aws.StringValue(grants[1].Grantee.ID))
				permission := s3_constants.PermissionRead
				if tt.acl == "bucket-owner-full-control" {
					permission = s3_constants.PermissionFullControl
				}
				require.Equal(t, permission, aws.StringValue(grants[1].Permission))
			}
		})
	}
}
