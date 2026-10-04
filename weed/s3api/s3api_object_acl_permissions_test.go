package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
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

// TestPutObjectAclPermissions 通过签名请求验证桶和前缀授权，以及管理员修改时的拥有者授权。
func TestPutObjectAclPermissions(t *testing.T) {
	const bucket, object, owner = "acl-bucket", "allowed/image.png", "object-owner"
	tests := []struct {
		name    string
		account string
		action  Action
		acl     string
		status  int
	}{
		{"桶内授权", owner, "WriteAcp:acl-bucket", "private", http.StatusOK},
		{"前缀内授权", owner, "WriteAcp:acl-bucket/allowed/*", "public-read", http.StatusOK},
		{"前缀外拒绝", owner, "WriteAcp:acl-bucket/other/*", "private", http.StatusForbidden},
		{"其他桶拒绝", owner, "WriteAcp:other-bucket", "private", http.StatusForbidden},
		{"只读权限拒绝", owner, "Read:acl-bucket", "private", http.StatusForbidden},
		{"非拥有者拒绝", "other-owner", "WriteAcp:acl-bucket", "private", http.StatusForbidden},
		{"管理员设置私有", AccountAdmin.Id, "Admin", "private", http.StatusOK},
		{"管理员设置公共读", AccountAdmin.Id, "Admin", "public-read", http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filer := &objectACLUpdateFiler{fakeLookupFiler: fakeLookupFiler{entry: &filer_pb.Entry{
				Name: "image.png", Extended: map[string][]byte{s3_constants.ExtAmzOwnerKey: []byte(owner)},
			}}}
			s3a := newHeadBucketTestServer(t, filer)
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			s3a.bucketConfigCache.Set(bucket, &BucketConfig{Name: bucket, Ownership: s3_constants.OwnershipObjectWriter, Owner: owner})
			s3a.iam = NewIdentityAccessManagementWithStore(s3a.option, nil, "memory")
			t.Cleanup(s3a.iam.Shutdown)
			s3a.iam.isAuthEnabled = true
			account := &Account{Id: tt.account, DisplayName: tt.account}
			identity := &Identity{
				Name: "acl-test-user", Account: account, Actions: []Action{tt.action}, IsStatic: true,
				Credentials: []*Credential{{AccessKey: routingTestAccessKey, SecretKey: routingTestSecretKey}},
			}
			s3a.iam.accessKeyIdent[routingTestAccessKey] = identity
			s3a.iam.nameToIdentity[identity.Name] = identity
			s3a.iam.accounts[owner] = &Account{Id: owner, DisplayName: owner}
			s3a.iam.accounts[account.Id] = account

			req := httptest.NewRequest(http.MethodPut, "http://s3/"+bucket+"/"+object+"?acl", nil)
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": object})
			req.Header.Set(s3_constants.AmzCannedAcl, tt.acl)
			signRoutingTestRequest(t, req, "", "s3")
			// 签名助手会替换正文；还原真实HTTP服务端的无正文请求，避免被当成空XML。
			req.Body = http.NoBody
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
			require.Equal(t, owner, string(update.Entry.Extended[s3_constants.ExtAmzOwnerKey]))
			grants := GetAcpGrants(update.Entry.Extended)
			wantGrants := 1
			if tt.acl == "public-read" {
				wantGrants = 2
			}
			require.Len(t, grants, wantGrants)
			require.Equal(t, owner, aws.StringValue(grants[0].Grantee.ID), "完全控制授权必须保留给原拥有者")
			require.Equal(t, s3_constants.PermissionFullControl, aws.StringValue(grants[0].Permission))
			if tt.acl == "public-read" {
				require.Equal(t, s3_constants.GranteeGroupAllUsers, aws.StringValue(grants[1].Grantee.URI))
				require.Equal(t, s3_constants.PermissionRead, aws.StringValue(grants[1].Permission))
			}
		})
	}
}
