package s3api

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/iam_pb"
	xhttp "github.com/chrislusf/seaweedfs/weed/s3api/http"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3_constants"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
)

type Action string

type Iam interface {
	Check(f http.HandlerFunc, actions ...Action) http.HandlerFunc
}

type IdentityAccessManagement struct {
	m sync.RWMutex

	identities []*Identity
	domain     string
}

type Identity struct {
	Name        string
	Credentials []*Credential
	Actions     []Action
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
	err = pb.WithFilerClient(false, option.Filer, option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		content, err = filer.ReadInsideFiler(client, filer.IamConfigDirecotry, filer.IamIdentityFile)
		return err
	})
	if err != nil {
		return fmt.Errorf("read S3 config: %v", err)
	}
	return iam.loadS3ApiConfigurationFromBytes(content)
}

func (iam *IdentityAccessManagement) loadS3ApiConfigurationFromFile(fileName string) error {
	content, readErr := os.ReadFile(fileName)
	if readErr != nil {
		glog.Warningf("fail to read %s : %v", fileName, readErr)
		return fmt.Errorf("fail to read %s : %v", fileName, readErr)
	}
	return iam.loadS3ApiConfigurationFromBytes(content)
}

func (iam *IdentityAccessManagement) loadS3ApiConfigurationFromBytes(content []byte) error {
	s3ApiConfiguration := &iam_pb.S3ApiConfiguration{}
	if err := filer.ParseS3ConfigurationFromBytes(content, s3ApiConfiguration); err != nil {
		glog.Warningf("unmarshal error: %v", err)
		return fmt.Errorf("unmarshal error: %v", err)
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
			Credentials: nil,
			Actions:     nil,
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
	iam.m.Unlock()
	return nil
}

func (iam *IdentityAccessManagement) isEnabled() bool {
	iam.m.RLock()
	defer iam.m.RUnlock()
	return len(iam.identities) > 0
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
		if ident.Name == "anonymous" {
			return ident, true
		}
	}
	return nil, false
}

func (iam *IdentityAccessManagement) Auth(f http.HandlerFunc, action Action) http.HandlerFunc {

	if !iam.isEnabled() {
		return f
	}

	return func(w http.ResponseWriter, r *http.Request) {
		identity, errCode := iam.authRequest(r, action)
		if errCode == s3err.ErrNone {
			if identity != nil && identity.Name != "" {
				r.Header.Set(xhttp.AmzIdentityId, identity.Name)
				if identity.isAdmin() {
					r.Header.Set(xhttp.AmzIsAdmin, "true")
				} else if _, ok := r.Header[xhttp.AmzIsAdmin]; ok {
					r.Header.Del(xhttp.AmzIsAdmin)
				}
			}
			f(w, r)
			return
		}
		s3err.WriteErrorResponse(w, r, errCode)
	}
}

// check whether the request has valid access keys
func (iam *IdentityAccessManagement) authRequest(r *http.Request, action Action) (*Identity, s3err.ErrorCode) {
	var identity *Identity
	var s3Err s3err.ErrorCode
	var found bool
	var authType string
	switch getRequestAuthType(r) {
	case authTypeStreamingSigned:
		return identity, s3err.ErrNone
	case authTypeUnknown:
		glog.V(3).Infof("unknown auth type")
		r.Header.Set(xhttp.AmzAuthType, "Unknown")
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
		r.Header.Set(xhttp.AmzAuthType, "PostPolicy")
		return identity, s3err.ErrNone
	case authTypeJWT:
		glog.V(3).Infof("jwt auth type")
		r.Header.Set(xhttp.AmzAuthType, "Jwt")
		return identity, s3err.ErrNotImplemented
	case authTypeAnonymous:
		authType = "Anonymous"
		identity, found = iam.lookupAnonymous()
		if !found {
			r.Header.Set(xhttp.AmzAuthType, authType)
			return identity, s3err.ErrAccessDenied
		}
	default:
		return identity, s3err.ErrNotImplemented
	}

	if len(authType) > 0 {
		r.Header.Set(xhttp.AmzAuthType, authType)
	}
	if s3Err != s3err.ErrNone {
		return identity, s3Err
	}

	glog.V(3).Infof("user name: %v actions: %v, action: %v", identity.Name, identity.Actions, action)

	bucket, object := xhttp.GetBucketAndObject(r)

	if !identity.canDo(action, bucket, object) {
		return identity, s3err.ErrAccessDenied
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
		r.Header.Set(xhttp.AmzAuthType, "Unknown")
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
		r.Header.Set(xhttp.AmzAuthType, "PostPolicy")
		return identity, s3err.ErrNone
	case authTypeJWT:
		glog.V(3).Infof("jwt auth type")
		r.Header.Set(xhttp.AmzAuthType, "Jwt")
		return identity, s3err.ErrNotImplemented
	case authTypeAnonymous:
		authType = "Anonymous"
		identity, found = iam.lookupAnonymous()
		if !found {
			r.Header.Set(xhttp.AmzAuthType, authType)
			return identity, s3err.ErrAccessDenied
		}
	default:
		return identity, s3err.ErrNotImplemented
	}

	if len(authType) > 0 {
		r.Header.Set(xhttp.AmzAuthType, authType)
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
