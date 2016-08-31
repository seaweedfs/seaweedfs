package mysql_store

import (
	"database/sql"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	sqlUrl             = "%s:%s@tcp(%s:%d)/%s?charset=utf8"
	maxIdleConnections = 100
	maxOpenConnections = 50
	maxTableNums       = 1024
	tableName          = "filer_mapping"
)

var (
	_init_db        sync.Once
	_db_connections []*sql.DB
)

type MySqlConf struct {
	User     string
	Password string
	HostName string
	Port     int
	DataBase string
}

type MySqlStore struct {
	dbs []*sql.DB
}

func getDbConnection(confs []MySqlConf) []*sql.DB {
	_init_db.Do(func() {
		for _, conf := range confs {

			sqlUrl := fmt.Sprintf(sqlUrl, conf.User, conf.Password, conf.HostName, conf.Port, conf.DataBase)
			var dbErr error
			_db_connection, dbErr := sql.Open("mysql", sqlUrl)
			if dbErr != nil {
				_db_connection.Close()
				_db_connection = nil
				panic(dbErr)
			}
			_db_connection.SetMaxIdleConns(maxIdleConnections)
			_db_connection.SetMaxOpenConns(maxOpenConnections)
			_db_connections = append(_db_connections, _db_connection)
		}
	})
	return _db_connections
}

func NewMysqlStore(confs []MySqlConf) *MySqlStore {
	ms := &MySqlStore{
		dbs: getDbConnection(confs),
	}

	for _, db := range ms.dbs {
		for i := 0; i < maxTableNums; i++ {
			if err := ms.createTables(db, tableName, i); err != nil {
				fmt.Printf("create table failed %s", err.Error())
			}
		}
	}

	return ms
}

func (s *MySqlStore) hash(fullFileName string) (instance_offset, table_postfix int) {
	hash_value := crc32.ChecksumIEEE([]byte(fullFileName))
	instance_offset = int(hash_value) % len(s.dbs)
	table_postfix = int(hash_value) % maxTableNums
	return
}

func (s *MySqlStore) parseFilerMappingInfo(path string) (instanceId int, tableFullName string, err error) {
	instance_offset, table_postfix := s.hash(path)
	instanceId = instance_offset
	tableFullName = fmt.Sprintf("%s_%04d", tableName, table_postfix)
	return
}

func (s *MySqlStore) Get(fullFilePath string) (fid string, err error) {
	instance_offset, tableFullName, err := s.parseFilerMappingInfo(fullFilePath)
	if err != nil {
		return "", err
	}
	fid, err = s.query(fullFilePath, s.dbs[instance_offset], tableFullName)
	if err == sql.ErrNoRows {
		//Could not found
		err = nil
	}
	return fid, err
}

func (s *MySqlStore) Put(fullFilePath string, fid string) (err error) {
	var tableFullName string

	instance_offset, tableFullName, err := s.parseFilerMappingInfo(fullFilePath)
	if err != nil {
		return err
	}
	if old_fid, localErr := s.query(fullFilePath, s.dbs[instance_offset], tableFullName); localErr != nil && localErr != sql.ErrNoRows {
		err = localErr
		return
	} else {
		if len(old_fid) == 0 {
			err = s.insert(fullFilePath, fid, s.dbs[instance_offset], tableFullName)
		} else {
			err = s.update(fullFilePath, fid, s.dbs[instance_offset], tableFullName)
		}
	}
	return
}

func (s *MySqlStore) Delete(fullFilePath string) (err error) {
	var fid string
	instance_offset, tableFullName, err := s.parseFilerMappingInfo(fullFilePath)
	if err != nil {
		return err
	}
	if fid, err = s.query(fullFilePath, s.dbs[instance_offset], tableFullName); err != nil {
		return err
	} else if fid == "" {
		return nil
	}
	if err := s.delete(fullFilePath, s.dbs[instance_offset], tableFullName); err != nil {
		return err
	} else {
		return nil
	}
}

func (s *MySqlStore) Close() {
	for _, db := range s.dbs {
		db.Close()
	}
}

var createTable = `
CREATE TABLE IF NOT EXISTS %s_%04d (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  uriPath char(256) NOT NULL DEFAULT "" COMMENT 'http uriPath',
  fid char(36) NOT NULL DEFAULT "" COMMENT 'seaweedfs fid',
  createTime int(10) NOT NULL DEFAULT 0 COMMENT 'createdTime in unix timestamp',
  updateTime int(10) NOT NULL DEFAULT 0 COMMENT 'updatedTime in unix timestamp',
  remark varchar(20) NOT NULL DEFAULT "" COMMENT 'reserverd field',
  status tinyint(2) DEFAULT '1' COMMENT 'resource status',
  PRIMARY KEY (id),
  UNIQUE KEY index_uriPath (uriPath)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
`

func (s *MySqlStore) createTables(db *sql.DB, tableName string, postfix int) error {
	stmt, err := db.Prepare(fmt.Sprintf(createTable, tableName, postfix))
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	return nil
}

func (s *MySqlStore) query(uriPath string, db *sql.DB, tableName string) (string, error) {
	sqlStatement := "SELECT fid FROM %s WHERE uriPath=?"
	row := db.QueryRow(fmt.Sprintf(sqlStatement, tableName), uriPath)
	var fid string
	err := row.Scan(&fid)
	if err != nil {
		return "", err
	}
	return fid, nil
}

func (s *MySqlStore) update(uriPath string, fid string, db *sql.DB, tableName string) error {
	sqlStatement := "UPDATE %s SET fid=?, updateTime=? WHERE uriPath=?"
	res, err := db.Exec(fmt.Sprintf(sqlStatement, tableName), fid, time.Now().Unix(), uriPath)
	if err != nil {
		return err
	}

	_, err = res.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}

func (s *MySqlStore) insert(uriPath string, fid string, db *sql.DB, tableName string) error {
	sqlStatement := "INSERT INTO %s (uriPath,fid,createTime) VALUES(?,?,?)"
	res, err := db.Exec(fmt.Sprintf(sqlStatement, tableName), uriPath, fid, time.Now().Unix())
	if err != nil {
		return err
	}

	_, err = res.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}

func (s *MySqlStore) delete(uriPath string, db *sql.DB, tableName string) error {
	sqlStatement := "DELETE FROM %s WHERE uriPath=?"
	res, err := db.Exec(fmt.Sprintf(sqlStatement, tableName), uriPath)
	if err != nil {
		return err
	}

	_, err = res.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}
