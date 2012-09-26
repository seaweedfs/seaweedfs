package operation

import (
	"net/http"
)

func Delete(url string) error {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	_, err = http.DefaultClient.Do(req)
	return err
}
