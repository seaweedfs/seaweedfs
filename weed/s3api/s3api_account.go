package s3api

import "sync"

var (
	AccountAdminName     = "admin"
	AccountAdminId       = "4a896effce63e6eaae3efe15718a56f9543381708ad1c8ee5f31df59c827b832"
	AccountAnonymousName = "anonymous"
	AccountAnonymousId   = "65a011a29cdf8ec533ec3d1ccaae921c"

	AccountAdmin = &Account{
		Name:         AccountAdminName,
		EmailAddress: "admin@example.com",
		CanonicalId:  AccountAdminId,
	}
	AccountAnonymous = &Account{
		Name:         AccountAnonymousName,
		EmailAddress: "anonymous@example.com",
		CanonicalId:  AccountAnonymousId,
	}
)

type Account struct {
	Name         string
	EmailAddress string
	CanonicalId  string
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
	for _, account := range []*Account{AccountAdmin, AccountAnonymous} {
		am.IdNameMapping[account.CanonicalId] = &account.Name
		am.EmailIdMapping[account.EmailAddress] = &account.CanonicalId
	}
}
