package s3api

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"io/ioutil"
	"net/http"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/iam_pb"
	xhttp "github.com/chrislusf/seaweedfs/weed/s3api/http"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
)

type Action string

type Iam interface {
	Check(f http.HandlerFunc, actions ...Action) http.HandlerFunc
}

type IdentityAccessManagement struct {
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

func (iam *IdentityAccessManagement) loadS3ApiConfigurationFromFiler(option *S3ApiServerOption) error {
	content, err := filer.ReadContent(option.Filer, filer.IamConfigDirecotry, filer.IamIdentityFile)
	if err != nil {
		return fmt.Errorf("read S3 config: %v", err)
	}
	return iam.loadS3ApiConfigurationFromBytes(content)
}

func (iam *IdentityAccessManagement) loadS3ApiConfigurationFromFile(fileName string) error {
	content, readErr := ioutil.ReadFile(fileName)
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

	// atomically switch
	iam.identities = identities
	return nil
}

func (iam *IdentityAccessManagement) isEnabled() bool {

	return len(iam.identities) > 0
}

func (iam *IdentityAccessManagement) lookupByAccessKey(accessKey string) (identity *Identity, cred *Credential, found bool) {

	for _, ident := range iam.identities {
		for _, cred := range ident.Credentials {
			if cred.AccessKey == accessKey {
				return ident, cred, true
			}
		}
	}
	return nil, nil, false
}

func (iam *IdentityAccessManagement) lookupAnonymous() (identity *Identity, found bool) {

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
				}
			}
			f(w, r)
			return
		}
		writeErrorResponse(w, errCode, r.URL)
	}
}

// check whether the request has valid access keys
func (iam *IdentityAccessManagement) authRequest(r *http.Request, action Action) (*Identity, s3err.ErrorCode) {
	var identity *Identity
	var s3Err s3err.ErrorCode
	var found bool
	switch getRequestAuthType(r) {
	case authTypeStreamingSigned:
		return identity, s3err.ErrNone
	case authTypeUnknown:
		glog.V(3).Infof("unknown auth type")
		return identity, s3err.ErrAccessDenied
	case authTypePresignedV2, authTypeSignedV2:
		glog.V(3).Infof("v2 auth type")
		identity, s3Err = iam.isReqAuthenticatedV2(r)
	case authTypeSigned, authTypePresigned:
		glog.V(3).Infof("v4 auth type")
		identity, s3Err = iam.reqSignatureV4Verify(r)
	case authTypePostPolicy:
		glog.V(3).Infof("post policy auth type")
		return identity, s3err.ErrNone
	case authTypeJWT:
		glog.V(3).Infof("jwt auth type")
		return identity, s3err.ErrNotImplemented
	case authTypeAnonymous:
		identity, found = iam.lookupAnonymous()
		if !found {
			return identity, s3err.ErrAccessDenied
		}
	default:
		return identity, s3err.ErrNotImplemented
	}

	glog.V(3).Infof("auth error: %v", s3Err)
	if s3Err != s3err.ErrNone {
		return identity, s3Err
	}

	glog.V(3).Infof("user name: %v actions: %v", identity.Name, identity.Actions)

	bucket, _ := getBucketAndObject(r)

	if !identity.canDo(action, bucket) {
		return identity, s3err.ErrAccessDenied
	}

	return identity, s3err.ErrNone

}

func (identity *Identity) canDo(action Action, bucket string) bool {
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
	limitedByBucket := string(action) + ":" + bucket
	for _, a := range identity.Actions {
		if string(a) == limitedByBucket {
			return true
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
