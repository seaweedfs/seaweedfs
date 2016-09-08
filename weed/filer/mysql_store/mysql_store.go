package mysql_store

import (
	"database/sql"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"

	_ "github.com/go-sql-driver/mysql"
)

const (
	sqlUrl                     = "%s:%s@tcp(%s:%d)/%s?charset=utf8"
	default_maxIdleConnections = 100
	default_maxOpenConnections = 50
	default_maxTableNums       = 1024
	tableName                  = "filer_mapping"
)

var (
	_init_db        sync.Once
	_db_connections []*sql.DB
)

type MySqlConf struct {
	User               string
	Password           string
	HostName           string
	Port               int
	DataBase           string
	MaxIdleConnections int
	MaxOpenConnections int
}

type ShardingConf struct {
	IsSharding bool `json:"isSharding"`
	ShardCount int  `json:"shardCount"`
}

type MySqlStore struct {
	dbs        []*sql.DB
	isSharding bool
	shardCount int
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
			var maxIdleConnections, maxOpenConnections int

			if conf.MaxIdleConnections != 0 {
				maxIdleConnections = conf.MaxIdleConnections
			} else {
				maxIdleConnections = default_maxIdleConnections
			}
			if conf.MaxOpenConnections != 0 {
				maxOpenConnections = conf.MaxOpenConnections
			} else {
				maxOpenConnections = default_maxOpenConnections
			}

			_db_connection.SetMaxIdleConns(maxIdleConnections)
			_db_connection.SetMaxOpenConns(maxOpenConnections)
			_db_connections = append(_db_connections, _db_connection)
		}
	})
	return _db_connections
}

func NewMysqlStore(confs []MySqlConf, isSharding bool, shardCount int) *MySqlStore {
	ms := &MySqlStore{
		dbs:        getDbConnection(confs),
		isSharding: isSharding,
		shardCount: shardCount,
	}

	for _, db := range ms.dbs {
		if !isSharding {
			ms.shardCount = 1
		} else {
			if ms.shardCount == 0 {
				ms.shardCount = default_maxTableNums
			}
		}
		for i := 0; i < ms.shardCount; i++ {
			if err := ms.createTables(db, tableName, i); err != nil {
				fmt.Printf("create table failed %v", err)
			}
		}
	}

	return ms
}

func (s *MySqlStore) hash(fullFileName string) (instance_offset, table_postfix int) {
	hash_value := crc32.ChecksumIEEE([]byte(fullFileName))
	instance_offset = int(hash_value) % len(s.dbs)
	table_postfix = int(hash_value) % s.shardCount
	return
}

func (s *MySqlStore) parseFilerMappingInfo(path string) (instanceId int, tableFullName string, err error) {
	instance_offset, table_postfix := s.hash(path)
	instanceId = instance_offset
	if s.isSharding {
		tableFullName = fmt.Sprintf("%s_%04d", tableName, table_postfix)
	} else {
		tableFullName = tableName
	}
	return
}

func (s *MySqlStore) Get(fullFilePath string) (fid string, err error) {
	instance_offset, tableFullName, err := s.parseFilerMappingInfo(fullFilePath)
	if err != nil {
		return "", fmt.Errorf("MySqlStore Get operation can not parse file path %s: err is %v", fullFilePath, err)
	}
	fid, err = s.query(fullFilePath, s.dbs[instance_offset], tableFullName)
	if err == sql.ErrNoRows {
		//Could not found
		err = filer.ErrNotFound
	}
	return fid, err
}

func (s *MySqlStore) Put(fullFilePath string, fid string) (err error) {
	var tableFullName string

	instance_offset, tableFullName, err := s.parseFilerMappingInfo(fullFilePath)
	if err != nil {
		return fmt.Errorf("MySqlStore Put operation can not parse file path %s: err is %v", fullFilePath, err)
	}
	var old_fid string
	if old_fid, err = s.query(fullFilePath, s.dbs[instance_offset], tableFullName); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("MySqlStore Put operation failed when querying path %s: err is %v", fullFilePath, err)
	} else {
		if len(old_fid) == 0 {
			err = s.insert(fullFilePath, fid, s.dbs[instance_offset], tableFullName)
			err = fmt.Errorf("MySqlStore Put operation failed when inserting path %s with fid %s : err is %v", fullFilePath, fid, err)
		} else {
			err = s.update(fullFilePath, fid, s.dbs[instance_offset], tableFullName)
			err = fmt.Errorf("MySqlStore Put operation failed when updating path %s with fid %s : err is %v", fullFilePath, fid, err)
		}
	}
	return
}

func (s *MySqlStore) Delete(fullFilePath string) (err error) {
	var fid string
	instance_offset, tableFullName, err := s.parseFilerMappingInfo(fullFilePath)
	if err != nil {
		return fmt.Errorf("MySqlStore Delete operation can not parse file path %s: err is %v", fullFilePath, err)
	}
	if fid, err = s.query(fullFilePath, s.dbs[instance_offset], tableFullName); err != nil {
		return fmt.Errorf("MySqlStore Delete operation failed when querying path %s: err is %v", fullFilePath, err)
	} else if fid == "" {
		return nil
	}
	if err = s.delete(fullFilePath, s.dbs[instance_offset], tableFullName); err != nil {
		return fmt.Errorf("MySqlStore Delete operation failed when deleting path %s: err is %v", fullFilePath, err)
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
CREATE TABLE IF NOT EXISTS %s (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  uriPath char(256) NOT NULL DEFAULT "" COMMENT 'http uriPath',
  fid char(36) NOT NULL DEFAULT "" COMMENT 'seaweedfs fid',
  createTime int(10) NOT NULL DEFAULT 0 COMMENT 'createdTime in unix timestamp',
  updateTime int(10) NOT NULL DEFAULT 0 COMMENT 'updatedTime in unix timestamp',
  remark varchar(20) NOT NULL DEFAULT "" COMMENT 'reserverd field',
  status tinyint(2) DEFAULT '1' COMMENT 'resource status',
  PRIMARY KEY (id),
  UNIQUE KEY index_uriPath (uriPath)
) DEFAULT CHARSET=utf8;
`

func (s *MySqlStore) createTables(db *sql.DB, tableName string, postfix int) error {
	var realTableName string
	if s.isSharding {
		realTableName = fmt.Sprintf("%s_%4d", tableName, postfix)
	} else {
		realTableName = tableName
	}

	stmt, err := db.Prepare(fmt.Sprintf(createTable, realTableName))
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
