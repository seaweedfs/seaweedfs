package s3account

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"sync"
)

//Predefined Accounts
var (
	// AccountAdmin is used as the default account for IAM-Credentials access without Account configured
	AccountAdmin = Account{
		Name:         "admin",
		EmailAddress: "admin@example.com",
		Id:           "admin",
	}

	// AccountAnonymous is used to represent the account for anonymous access
	AccountAnonymous = Account{
		Name:         "anonymous",
		EmailAddress: "anonymous@example.com",
		Id:           "anonymous",
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

	//Id is used to identify an Account when granting cross-account access(ACLs) to buckets and objects
	Id string
}

type AccountManager struct {
	sync.Mutex
	filerClient filer_pb.FilerClient

	IdNameMapping  map[string]string
	EmailIdMapping map[string]string
}

func NewAccountManager(filerClient filer_pb.FilerClient) *AccountManager {
	am := &AccountManager{
		filerClient:    filerClient,
		IdNameMapping:  make(map[string]string),
		EmailIdMapping: make(map[string]string),
	}
	am.initialize()
	return am
}

func (am *AccountManager) GetAccountNameById(canonicalId string) string {
	return am.IdNameMapping[canonicalId]
}

func (am *AccountManager) GetAccountIdByEmail(email string) string {
	return am.EmailIdMapping[email]
}

func (am *AccountManager) initialize() {
	// load predefined Accounts
	for _, account := range []Account{AccountAdmin, AccountAnonymous} {
		am.IdNameMapping[account.Id] = account.Name
		am.EmailIdMapping[account.EmailAddress] = account.Id
	}
}
