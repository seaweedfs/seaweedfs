package s3api

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3account"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

var IdentityAnonymous = &Identity{
	Name:      s3account.AccountAnonymous.Name,
	AccountId: s3account.AccountAnonymous.Id,
}

type Action string

type Iam interface {
	Check(f http.HandlerFunc, actions ...Action) http.HandlerFunc
}

type IdentityAccessManagement struct {
	m sync.RWMutex

	identities    []*Identity
	isAuthEnabled bool
	domain        string
}

type Identity struct {
	Name        string
	AccountId   string
	Credentials []*Credential
	Actions     []Action
}

func (i *Identity) isAnonymous() bool {
	return i.Name == s3account.AccountAnonymous.Name
}

type Credential struct {
	AccessKey string
	SecretKey string
}

func (action Action) isAdmin() bool {
	return strings.HasPrefix(string(action), s3_constants.ACTION_ADMIN)
}

func (action Action) isOwner(bucket string) bool {
	return string(action) == s3_constants.ACTION_ADMIN+":"+bucket
}

func (action Action) overBucket(bucket string) bool {
	return strings.HasSuffix(string(action), ":"+bucket) || strings.HasSuffix(string(action), ":*")
}

func (action Action) getPermission() Permission {
	switch act := strings.Split(string(action), ":")[0]; act {
	case s3_constants.ACTION_ADMIN:
		return Permission("FULL_CONTROL")
	case s3_constants.ACTION_WRITE:
		return Permission("WRITE")
	case s3_constants.ACTION_READ:
		return Permission("READ")
	default:
		return Permission("")
	}
}

func NewIdentityAccessManagement(option *S3ApiServerOption) *IdentityAccessManagement {
	iam := &IdentityAccessManagement{
		domain: option.DomainName,
	}
	if option.Config != "" {
		if err := iam.loadS3ApiConfigurationFromFile(option.Config); err != nil {
			glog.Fatalf("fail to load config file %s: %v", option.Config, err)
		}
	} else {
		if err := iam.loadS3ApiConfigurationFromFiler(option); err != nil {
			glog.Warningf("fail to load config: %v", err)
		}
	}
	return iam
}

func (iam *IdentityAccessManagement) loadS3ApiConfigurationFromFiler(option *S3ApiServerOption) (err error) {
	var content []byte
	err = pb.WithFilerClient(false, 0, option.Filer, option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		content, err = filer.ReadInsideFiler(client, filer.IamConfigDirectory, filer.IamIdentityFile)
		return err
	})
	if err != nil {
		return fmt.Errorf("read S3 config: %v", err)
	}
	return iam.LoadS3ApiConfigurationFromBytes(content)
}

func (iam *IdentityAccessManagement) loadS3ApiConfigurationFromFile(fileName string) error {
	content, readErr := os.ReadFile(fileName)
	if readErr != nil {
		glog.Warningf("fail to read %s : %v", fileName, readErr)
		return fmt.Errorf("fail to read %s : %v", fileName, readErr)
	}
	return iam.LoadS3ApiConfigurationFromBytes(content)
}

func (iam *IdentityAccessManagement) LoadS3ApiConfigurationFromBytes(content []byte) error {
	s3ApiConfiguration := &iam_pb.S3ApiConfiguration{}
	if err := filer.ParseS3ConfigurationFromBytes(content, s3ApiConfiguration); err != nil {
		glog.Warningf("unmarshal error: %v", err)
		return fmt.Errorf("unmarshal error: %v", err)
	}

	if err := filer.CheckDuplicateAccessKey(s3ApiConfiguration); err != nil {
		return err
	}

	if err := iam.loadS3ApiConfiguration(s3ApiConfiguration); err != nil {
		return err
	}
	return nil
}

func (iam *IdentityAccessManagement) loadS3ApiConfiguration(config *iam_pb.S3ApiConfiguration) error {
	var identities []*Identity
	for _, ident := range config.Identities {
		t := &Identity{
			Name:        ident.Name,
			AccountId:   s3account.AccountAdmin.Id,
			Credentials: nil,
			Actions:     nil,
		}

		if ident.Name == s3account.AccountAnonymous.Name {
			if ident.AccountId != "" && ident.AccountId != s3account.AccountAnonymous.Id {
				glog.Warningf("anonymous identity is associated with a non-anonymous account ID, the association is invalid")
			}
			t.AccountId = s3account.AccountAnonymous.Id
			IdentityAnonymous = t
		} else {
			if len(ident.AccountId) > 0 {
				t.AccountId = ident.AccountId
			}
		}

		for _, action := range ident.Actions {
			t.Actions = append(t.Actions, Action(action))
		}
		for _, cred := range ident.Credentials {
			t.Credentials = append(t.Credentials, &Credential{
				AccessKey: cred.AccessKey,
				SecretKey: cred.SecretKey,
			})
		}
		identities = append(identities, t)
	}

	iam.m.Lock()
	// atomically switch
	iam.identities = identities
	if !iam.isAuthEnabled { // one-directional, no toggling
		iam.isAuthEnabled = len(identities) > 0
	}
	iam.m.Unlock()
	return nil
}

func (iam *IdentityAccessManagement) isEnabled() bool {
	return iam.isAuthEnabled
}

func (iam *IdentityAccessManagement) lookupByAccessKey(accessKey string) (identity *Identity, cred *Credential, found bool) {

	iam.m.RLock()
	defer iam.m.RUnlock()
	for _, ident := range iam.identities {
		for _, cred := range ident.Credentials {
			// println("checking", ident.Name, cred.AccessKey)
			if cred.AccessKey == accessKey {
				return ident, cred, true
			}
		}
	}
	glog.V(1).Infof("could not find accessKey %s", accessKey)
	return nil, nil, false
}

func (iam *IdentityAccessManagement) lookupAnonymous() (identity *Identity, found bool) {
	iam.m.RLock()
	defer iam.m.RUnlock()
	for _, ident := range iam.identities {
		if ident.isAnonymous() {
			return ident, true
		}
	}
	return nil, false
}

func (iam *IdentityAccessManagement) Auth(f http.HandlerFunc, action Action) http.HandlerFunc {
	return Auth(iam, nil, f, action, false)
}

func (s3a *S3ApiServer) Auth(f http.HandlerFunc, action Action, supportAcl bool) http.HandlerFunc {
	return Auth(s3a.iam, s3a.bucketRegistry, f, action, supportAcl)
}

func Auth(iam *IdentityAccessManagement, br *BucketRegistry, f http.HandlerFunc, action Action, supportAcl bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		//unset predefined headers
		delete(r.Header, s3_constants.AmzAccountId)
		delete(r.Header, s3_constants.ExtAmzOwnerKey)
		delete(r.Header, s3_constants.ExtAmzAclKey)

		if !iam.isEnabled() {
			f(w, r)
			return
		}

		identity, errCode := authRequest(iam, br, r, action, supportAcl)
		if errCode == s3err.ErrNone {
			if identity != nil && identity.Name != "" {
				r.Header.Set(s3_constants.AmzIdentityId, identity.Name)
				if identity.isAdmin() {
					r.Header.Set(s3_constants.AmzIsAdmin, "true")
				} else if _, ok := r.Header[s3_constants.AmzIsAdmin]; ok {
					r.Header.Del(s3_constants.AmzIsAdmin)
				}
			}
			f(w, r)
			return
		}
		s3err.WriteErrorResponse(w, r, errCode)
	}
}

func (iam *IdentityAccessManagement) authRequest(r *http.Request, action Action) (*Identity, s3err.ErrorCode) {
	return authRequest(iam, nil, r, action, false)
}

func authRequest(iam *IdentityAccessManagement, br *BucketRegistry, r *http.Request, action Action, supportAcl bool) (*Identity, s3err.ErrorCode) {
	var identity *Identity
	var s3Err s3err.ErrorCode
	var authType string
	switch getRequestAuthType(r) {
	case authTypeStreamingSigned:
		return identity, s3err.ErrNone
	case authTypeUnknown:
		glog.V(3).Infof("unknown auth type")
		r.Header.Set(s3_constants.AmzAuthType, "Unknown")
		return identity, s3err.ErrAccessDenied
	case authTypePresignedV2, authTypeSignedV2:
		glog.V(3).Infof("v2 auth type")
		identity, s3Err = iam.isReqAuthenticatedV2(r)
		authType = "SigV2"
	case authTypeSigned, authTypePresigned:
		glog.V(3).Infof("v4 auth type")
		identity, s3Err = iam.reqSignatureV4Verify(r)
		authType = "SigV4"
	case authTypePostPolicy:
		glog.V(3).Infof("post policy auth type")
		r.Header.Set(s3_constants.AmzAuthType, "PostPolicy")
		return identity, s3err.ErrNone
	case authTypeJWT:
		glog.V(3).Infof("jwt auth type")
		r.Header.Set(s3_constants.AmzAuthType, "Jwt")
		return identity, s3err.ErrNotImplemented
	case authTypeAnonymous:
		if supportAcl && br != nil {
			bucket, _ := s3_constants.GetBucketAndObject(r)
			bucketMetadata, errorCode := br.GetBucketMetadata(bucket)
			if errorCode != s3err.ErrNone {
				return nil, errorCode
			}
			if bucketMetadata.ObjectOwnership != s3_constants.OwnershipBucketOwnerEnforced {
				return IdentityAnonymous, s3err.ErrNone
			}
		}
		authType = "Anonymous"
		identity = IdentityAnonymous
		if len(identity.Actions) == 0 {
			r.Header.Set(s3_constants.AmzAuthType, authType)
			return identity, s3err.ErrAccessDenied
		}
	default:
		return identity, s3err.ErrNotImplemented
	}

	if len(authType) > 0 {
		r.Header.Set(s3_constants.AmzAuthType, authType)
	}
	if s3Err != s3err.ErrNone {
		return identity, s3Err
	}

	glog.V(3).Infof("user name: %v account id: %v actions: %v, action: %v", identity.Name, identity.AccountId, identity.Actions, action)

	bucket, object := s3_constants.GetBucketAndObject(r)

	if !identity.canDo(action, bucket, object) {
		return identity, s3err.ErrAccessDenied
	}

	if !identity.isAnonymous() {
		r.Header.Set(s3_constants.AmzAccountId, identity.AccountId)
	}
	return identity, s3err.ErrNone

}

func (iam *IdentityAccessManagement) authUser(r *http.Request) (*Identity, s3err.ErrorCode) {
	var identity *Identity
	var s3Err s3err.ErrorCode
	var found bool
	var authType string
	switch getRequestAuthType(r) {
	case authTypeStreamingSigned:
		return identity, s3err.ErrNone
	case authTypeUnknown:
		glog.V(3).Infof("unknown auth type")
		r.Header.Set(s3_constants.AmzAuthType, "Unknown")
		return identity, s3err.ErrAccessDenied
	case authTypePresignedV2, authTypeSignedV2:
		glog.V(3).Infof("v2 auth type")
		identity, s3Err = iam.isReqAuthenticatedV2(r)
		authType = "SigV2"
	case authTypeSigned, authTypePresigned:
		glog.V(3).Infof("v4 auth type")
		identity, s3Err = iam.reqSignatureV4Verify(r)
		authType = "SigV4"
	case authTypePostPolicy:
		glog.V(3).Infof("post policy auth type")
		r.Header.Set(s3_constants.AmzAuthType, "PostPolicy")
		return identity, s3err.ErrNone
	case authTypeJWT:
		glog.V(3).Infof("jwt auth type")
		r.Header.Set(s3_constants.AmzAuthType, "Jwt")
		return identity, s3err.ErrNotImplemented
	case authTypeAnonymous:
		authType = "Anonymous"
		identity, found = iam.lookupAnonymous()
		if !found {
			r.Header.Set(s3_constants.AmzAuthType, authType)
			return identity, s3err.ErrAccessDenied
		}
	default:
		return identity, s3err.ErrNotImplemented
	}

	if len(authType) > 0 {
		r.Header.Set(s3_constants.AmzAuthType, authType)
	}

	glog.V(3).Infof("auth error: %v", s3Err)
	if s3Err != s3err.ErrNone {
		return identity, s3Err
	}
	return identity, s3err.ErrNone
}

func (identity *Identity) canDo(action Action, bucket string, objectKey string) bool {
	if identity.isAdmin() {
		return true
	}
	for _, a := range identity.Actions {
		if a == action {
			return true
		}
	}
	if bucket == "" {
		return false
	}
	target := string(action) + ":" + bucket + objectKey
	adminTarget := s3_constants.ACTION_ADMIN + ":" + bucket + objectKey
	limitedByBucket := string(action) + ":" + bucket
	adminLimitedByBucket := s3_constants.ACTION_ADMIN + ":" + bucket
	for _, a := range identity.Actions {
		act := string(a)
		if strings.HasSuffix(act, "*") {
			if strings.HasPrefix(target, act[:len(act)-1]) {
				return true
			}
			if strings.HasPrefix(adminTarget, act[:len(act)-1]) {
				return true
			}
		} else {
			if act == limitedByBucket {
				return true
			}
			if act == adminLimitedByBucket {
				return true
			}
		}
	}
	return false
}

func (identity *Identity) isAdmin() bool {
	for _, a := range identity.Actions {
		if a == "Admin" {
			return true
		}
	}
	return false
}
