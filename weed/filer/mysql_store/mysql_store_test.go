package mysql_store

import (
	"encoding/json"
	"hash/crc32"
	"testing"
)

/*
To improve performance when storing billion of files, you could shar
At each mysql instance, we will try to create 1024 tables if not exist, table name will be something like:
filer_mapping_0000
filer_mapping_0001
.....
filer_mapping_1023
sample conf should be

>$cat filer_conf.json
{
    "mysql": [
        {
            "User": "root",
            "Password": "root",
            "HostName": "127.0.0.1",
            "Port": 3306,
            "DataBase": "seaweedfs"
        },
        {
            "User": "root",
            "Password": "root",
            "HostName": "127.0.0.2",
            "Port": 3306,
            "DataBase": "seaweedfs"
        }
    ]
}
*/

func TestGenerateMysqlConf(t *testing.T) {
	var conf MySqlConf
	conf = append(conf, MySqlInstConf{
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
