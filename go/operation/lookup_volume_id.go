package operation

import (
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"errors"
	_ "fmt"
	"net/url"
	"strings"
)

type Location struct {
	Url       string `json:"url"`
	PublicUrl string `json:"publicUrl"`
}
type LookupResult struct {
	Locations []Location `json:"locations"`
	Error     string     `json:"error"`
}

func Lookup(server string, vid string) (*LookupResult, error) {
	values := make(url.Values)
	values.Add("volumeId", vid)
	jsonBlob, err := util.Post("http://"+server+"/dir/lookup", values)
	if err != nil {
		return nil, err
	}
	var ret LookupResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}

func LookupFileId(server string, fileId string) (fullUrl string, err error) {
	a := strings.Split(fileId, ",")
	if len(a) != 2 {
		return "", errors.New("Invalid fileId " + fileId)
	}
	lookup, lookupError := Lookup(server, a[0])
	if lookupError != nil {
		return "", lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", errors.New("File Not Found")
	}
	return "http://" + lookup.Locations[0].PublicUrl + "/" + fileId, nil
}
