package security

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

var (
	ErrUnauthorized = errors.New("unauthorized token")
)

/*
Guard is to ensure data access security.
There are 2 ways to check access:
1. white list. It's checking request ip address.
2. JSON Web Token(JWT) generated from secretKey.
  The jwt can come from:
  1. url parameter jwt=...
  2. request header "Authorization"
  3. cookie with the name "jwt"

The white list is checked first because it is easy.
Then the JWT is checked.

The Guard will also check these claims if provided:
1. "exp" Expiration Time
2. "nbf" Not Before

Generating JWT:
1. use HS256 to sign
2. optionally set "exp", "nbf" fields, in Unix time,
   the number of seconds elapsed since January 1, 1970 UTC.

Referenced:
https://github.com/pkieltyka/jwtauth/blob/master/jwtauth.go

*/

/*

Below sample config section should be modified per your needs and added into the filer.json config file if you want to
enable filer R/W API verfication like the AWS S3 RESTFUL authentication

{
   "appInfo": [
       {
           "AppKeyId": "XVNVWEYQVNGXWWNXGLKP",
           "AppKeySecret": "H8peDqD3A9Uc8KO28Iu99ywnW7TKQ8pii2PE8xpQ",
           "bucketList": [
               "yourbucketName"
           ]
       }
   ]
}

*/
type AppContent struct {
	AppKeySecret string
	BucketList   []string
}
type AppConf struct {
	AppContent
	AppKeyID string
}

type Guard struct {
	whiteList  []string
	SecretKey  Secret
	appKeyDict map[string]AppContent

	isActive bool
}

func NewGuard(whiteList []string, secretKey string, appConfs []AppConf) *Guard {
	appKeyDict := make(map[string]AppContent, len(appConfs))
	for _, appConf := range appConfs {
		appKeyDict[appConf.AppKeyID] = AppContent{
			AppKeySecret: appConf.AppKeySecret,
			BucketList:   appConf.BucketList,
		}
	}
	g := &Guard{whiteList: whiteList, SecretKey: Secret(secretKey), appKeyDict: appKeyDict}
	g.isActive = len(g.whiteList) != 0 || len(g.SecretKey) != 0 || len(g.appKeyDict) != 0
	return g
}

func (g *Guard) WhiteList(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	if !g.isActive {
		//if no security needed, just skip all checkings
		return f
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if err := g.checkWhiteList(w, r); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		f(w, r)
	}
}

func (g *Guard) Secure(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	if !g.isActive {
		//if no security needed, just skip all checkings
		return f
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if err := g.checkJwt(w, r); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		f(w, r)
	}
}

/*
Please refer to http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html for Rest Authentication details
*/
func (g *Guard) CheckAuthorization(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	if !g.isActive {
		//if no security needed, just skip all checkings
		return f
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if err := g.checkAuthorization(w, r); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		f(w, r)
	}
}

func GetActualRemoteHost(r *http.Request) (host string, err error) {
	host = r.Header.Get("HTTP_X_FORWARDED_FOR")
	if host == "" {
		host = r.Header.Get("X-FORWARDED-FOR")
	}
	if strings.Contains(host, ",") {
		host = host[0:strings.Index(host, ",")]
	}
	if host == "" {
		host, _, err = net.SplitHostPort(r.RemoteAddr)
	}
	return
}

func (g *Guard) checkWhiteList(w http.ResponseWriter, r *http.Request) error {
	if len(g.whiteList) == 0 {
		return nil
	}

	host, err := GetActualRemoteHost(r)
	if err == nil {
		for _, ip := range g.whiteList {

			// If the whitelist entry contains a "/" it
			// is a CIDR range, and we should check the
			// remote host is within it
			if strings.Contains(ip, "/") {
				_, cidrnet, err := net.ParseCIDR(ip)
				if err != nil {
					panic(err)
				}
				remote := net.ParseIP(host)
				if cidrnet.Contains(remote) {
					return nil
				}
			}

			//
			// Otherwise we're looking for a literal match.
			//
			if ip == host {
				return nil
			}
		}
	}

	glog.V(0).Infof("Not in whitelist: %s", r.RemoteAddr)
	return fmt.Errorf("Not in whitelis: %s", r.RemoteAddr)
}

func (g *Guard) checkJwt(w http.ResponseWriter, r *http.Request) error {
	if g.checkWhiteList(w, r) == nil {
		return nil
	}

	if len(g.SecretKey) == 0 {
		return nil
	}

	tokenStr := GetJwt(r)

	if tokenStr == "" {
		return ErrUnauthorized
	}

	// Verify the token
	token, err := DecodeJwt(g.SecretKey, tokenStr)
	if err != nil {
		glog.V(1).Infof("Token verification error from %s: %v", r.RemoteAddr, err)
		return ErrUnauthorized
	}
	if !token.Valid {
		glog.V(1).Infof("Token invliad from %s: %v", r.RemoteAddr, tokenStr)
		return ErrUnauthorized
	}

	glog.V(1).Infof("No permission from %s", r.RemoteAddr)
	return fmt.Errorf("No write permisson from %s", r.RemoteAddr)
}

func ComputeHmac256(message string, secret string) string {
	key := []byte(secret)
	h := hmac.New(sha256.New, key)
	h.Write([]byte(message))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

/*
authentication sample:
Authorization: AWS-HMAC-SHA256 PLLZOBTTZXGBNOWUFHZZ:tuXu/KcggHWPAfEmraUHDwEUdiIPSXVRsO+T2rxomBQ=
*/
func VerifyAuthorizationHeader(authorization string) (accessKeyId, signature string, err error) {
	authArr := strings.Split(authorization, " ")
	if len(authArr) != 2 {
		err = fmt.Errorf("authorization %s is not correct", authorization)
		return
	}

	/*
		This is just a sample, you could customize the signature algorithm freely
	*/
	if authArr[0] != "AWS-HMAC-SHA256" {
		err = fmt.Errorf("algorithm %s is not supported yet", authArr[0])
		return
	}

	sigArr := strings.Split(authArr[1], ":")
	if len(sigArr) != 2 {
		err = fmt.Errorf("signature %s is not correct", authArr[1])
		return
	}
	accessKeyId = sigArr[0]
	signature = sigArr[1]
	if len(accessKeyId) == 0 || len(signature) == 0 {
		err = fmt.Errorf("accessKeyId %s or signature %s is empty", accessKeyId, signature)
		return
	}
	return

}

func (g *Guard) GetAccessKeyAppContent(accessKeyId string) (AppContent, error) {

	if appContent, found := g.appKeyDict[accessKeyId]; !found {
		return AppContent{}, fmt.Errorf("%s key Not Found", accessKeyId)
	} else {
		return appContent, nil
	}
}

func GetBucketAndObjectName(r *http.Request) (bucketName, objectName string) {
	/*
	 http://host:port/objectName?collection=bucketName
	 http://host:port/bucketName/objectName
	*/

	collection := r.URL.Query().Get("collection")
	if collection != "" {
		return collection, r.URL.Path[1:]
	}

	secondPos := strings.Index(r.URL.Path[1:], "/")
	if secondPos == -1 {
		secondPos = len(r.URL.Path)
	} else {
		secondPos += 1
	}
	bucketName = r.URL.Path[1:secondPos]
	if secondPos+1 < len(r.URL.Path) {
		objectName = r.URL.Path[secondPos+1:]
	}
	return
}

type headerSorter struct {
	Keys []string
	Vals []string
}

func newHeaderSorter(m map[string]string) *headerSorter {
	hs := &headerSorter{
		Keys: make([]string, 0, len(m)),
		Vals: make([]string, 0, len(m)),
	}

	for k, v := range m {
		hs.Keys = append(hs.Keys, k)
		hs.Vals = append(hs.Vals, v)
	}
	return hs
}

func (hs *headerSorter) Sort() {
	sort.Sort(hs)
}

func (hs *headerSorter) Len() int {
	return len(hs.Vals)
}

func (hs *headerSorter) Less(i, j int) bool {
	return bytes.Compare([]byte(hs.Keys[i]), []byte(hs.Keys[j])) < 0
}

func (hs *headerSorter) Swap(i, j int) {
	hs.Vals[i], hs.Vals[j] = hs.Vals[j], hs.Vals[i]
	hs.Keys[i], hs.Keys[j] = hs.Keys[j], hs.Keys[i]
}

func (g *Guard) GenerateSignature(appKeySecret string, r *http.Request) (string, error) {
	unescaped_uri, err := url.QueryUnescape(r.RequestURI)
	if err != nil {
		glog.V(1).Infof("uri %s unescape err %s", unescaped_uri, err.Error())
		return "", ErrUnauthorized
	}

	temp := make(map[string]string)
	/*
		if there are some header options which were not prepared to added into the canonicalizedOSSHeaders,
		add a prefix as below and bypass
	*/
	for k, v := range r.Header {
		if !strings.HasPrefix(strings.ToLower(k), "x-aws-") {
			temp[strings.ToLower(k)] = v[0]
		}
	}
	hs := newHeaderSorter(temp)
	hs.Sort()

	// Get the CanonicalizedOSSHeaders
	canonicalizedOSSHeaders := ""
	for i := range hs.Keys {
		canonicalizedOSSHeaders += hs.Keys[i] + ":" + hs.Vals[i] + "\n"
	}

	string_to_sign := r.Method + "\n" +
		r.Header.Get("Content-MD5") + "\n" +
		r.Header.Get("Content-Type") + "\n" +
		r.Header.Get("Date") + "\n" +
		canonicalizedOSSHeaders +
		unescaped_uri

	return ComputeHmac256(string_to_sign, appKeySecret), nil
}

func (g *Guard) checkAuthorization(w http.ResponseWriter, r *http.Request) error {
	authorization_string_from_client := r.Header.Get("Authorization")
	if len(authorization_string_from_client) == 0 {
		glog.V(1).Infof("Authorization in header field is empty")
		return ErrUnauthorized
	}

	accessKeyId, signature, err := VerifyAuthorizationHeader(authorization_string_from_client)
	if err != nil {
		glog.V(1).Infof("verify authorization failed %s", err.Error())
		return ErrUnauthorized
	}

	appContent, err := g.GetAccessKeyAppContent(accessKeyId)
	if err != nil {
		glog.V(1).Infof("accessKeyId %s is invalid", accessKeyId)
		return ErrUnauthorized
	}

	bucketName, _ := GetBucketAndObjectName(r)

	var found bool
	for _, bucket := range appContent.BucketList {
		if bucket == bucketName {
			found = true
			break
		}
	}
	if !found {
		glog.V(1).Infof("bucketName %s is not authorized for appKeyId %s", bucketName, accessKeyId)
		return ErrUnauthorized
	}
	if my_sig, err := g.GenerateSignature(appContent.AppKeySecret, r); err != nil {
		return err
	} else {
		if signature != my_sig {
			glog.V(1).Infof("Authorization[%s]'s signature from client is not valid", authorization_string_from_client)
			return ErrUnauthorized
		} else {
			glog.V(3).Infof("Authorization succeeded!")
		}
	}

	return nil
}
