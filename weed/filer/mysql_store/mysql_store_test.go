package mysql_store

import (
	"encoding/json"
	"hash/crc32"
	"testing"
)

func TestGenerateMysqlConf(t *testing.T) {
	var conf []MySqlConf
	conf = append(conf, MySqlConf{
		User:     "root",
		Password: "root",
		HostName: "localhost",
		Port:     3306,
		DataBase: "seaweedfs",
	})
	body, err := json.Marshal(conf)
	if err != nil {
		t.Errorf("json encoding err %s", err.Error())
	}
	t.Logf("json output is %s", string(body))
}

func TestCRC32FullPathName(t *testing.T) {
	fullPathName := "/prod-bucket/law632191483895612493300-signed.pdf"
	hash_value := crc32.ChecksumIEEE([]byte(fullPathName))
	table_postfix := int(hash_value) % 1024
	t.Logf("table postfix %d", table_postfix)
}
