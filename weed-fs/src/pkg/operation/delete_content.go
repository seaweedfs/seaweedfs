package operation

import (
	"net/http"
	"log"
)

func Delete(url string) error {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Println("failing to delete", url)
		return err
	}
	_, err = http.DefaultClient.Do(req)
	return err
}
