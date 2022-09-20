package s3api

import (
	"github.com/seaweedfs/seaweedfs/weed/util"
	"sync"
)

//Predefined Accounts
var (
	AccountAdminName = "admin"
	// AccountAdmin is used as the default account for IAM-Credentials access without Account configured
	AccountAdmin = Account{
		Name:         AccountAdminName,
		EmailAddress: "admin@example.com",
		CanonicalId:  util.Md5String([]byte(AccountAdminName)),
	}

	AccountAnonymousName = "anonymous"
	// AccountAnonymous is used to represent the account for anonymous access
	AccountAnonymous = Account{
		Name:         AccountAnonymousName,
		EmailAddress: "anonymous@example.com",
		CanonicalId:  util.Md5String([]byte(AccountAnonymousName)),
	}
)

//Account represents a system user, a system user can
//configure multiple IAM-Users, IAM-Users can configure
//permissions respectively, and each IAM-User can
//configure multiple security credentials
type Account struct {
	//Name is also used to display the "DisplayName" as the owner of the bucket or object
	Name         string
	EmailAddress string

	//https://docs.aws.amazon.com/AmazonS3/latest/userguide/finding-canonical-user-id.html
	//The canonical user ID is an alpha-numeric identifier, such as '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be'
	//CanonicalId is used to identify an AWS account when granting cross-account access(ACLs) to buckets and objects using Amazon S3.
	CanonicalId string
}

type AccountManager struct {
	sync.Mutex
	s3a            *S3ApiServer
	IdNameMapping  map[string]*string
	EmailIdMapping map[string]*string
}

func NewAccountManager(s3a *S3ApiServer) *AccountManager {
	am := &AccountManager{
		s3a:            s3a,
		IdNameMapping:  make(map[string]*string),
		EmailIdMapping: make(map[string]*string),
	}
	am.initialize()
	return am
}

func (am *AccountManager) GetAccountName(canonicalId *string) *string {
	return am.IdNameMapping[*canonicalId]
}

func (am *AccountManager) initialize() {
	// Load Predefined Accounts
	for _, account := range []Account{AccountAdmin, AccountAnonymous} {
		am.IdNameMapping[account.CanonicalId] = &account.Name
		am.EmailIdMapping[account.EmailAddress] = &account.CanonicalId
	}

	// After the development of the Account-related interface is completed in the future, it is changed to be loaded from the filer here.
	// load from filer...
}
