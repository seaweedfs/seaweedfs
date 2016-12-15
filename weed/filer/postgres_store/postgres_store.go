package postgres_store

import (
	"database/sql"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"

	_ "github.com/lib/pq"
)

const (
	default_maxIdleConnections = 100
	default_maxOpenConnections = 50
	default_maxTableNums       = 1024
	tableName                  = "filer_mapping"
)

var (
	_init_db        sync.Once
	_db_connections []*sql.DB
)

type PostgresConf struct {
	User               string
	Password           string
	HostName           string
	Port               int
	DataBase           string
	SslMode            string
	MaxIdleConnections int
	MaxOpenConnections int
}

type ShardingConf struct {
	IsSharding bool `json:"isSharding"`
	ShardCount int  `json:"shardCount"`
}

type PostgresStore struct {
	dbs        []*sql.DB
	isSharding bool
	shardCount int
	server     string
	user       string
	password   string
}

func databaseExists(db *sql.DB, databaseName string) (bool, error) {
	sqlStatement := "SELECT datname from pg_database WHERE datname='%s'"
	row := db.QueryRow(fmt.Sprintf(sqlStatement, databaseName))

	var dbName string
	err := row.Scan(&dbName)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func createDatabase(db *sql.DB, databaseName string) error {
	sqlStatement := "CREATE DATABASE %s ENCODING='UTF8'"
	_, err := db.Exec(fmt.Sprintf(sqlStatement, databaseName))
	return err
}

func getDbConnection(confs []PostgresConf) []*sql.DB {
	_init_db.Do(func() {
		for _, conf := range confs {

			sqlUrl := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=30", conf.HostName, conf.Port, conf.User, conf.Password, "postgres", conf.SslMode)
			glog.V(3).Infoln("Opening postgres master database")

			var dbErr error
			_db_connection, dbErr := sql.Open("postgres", sqlUrl)
			if dbErr != nil {
				_db_connection.Close()
				_db_connection = nil
				panic(dbErr)
			}

			pingErr := _db_connection.Ping()
			if pingErr != nil {
				_db_connection.Close()
				_db_connection = nil
				panic(pingErr)
			}

			glog.V(3).Infoln("Checking to see if DB exists: ", conf.DataBase)
			var existsErr error
			dbExists, existsErr := databaseExists(_db_connection, conf.DataBase)
			if existsErr != nil {
				_db_connection.Close()
				_db_connection = nil
				panic(existsErr)
			}

			if !dbExists {
				glog.V(3).Infoln("Database doesn't exist. Attempting to create one:  ", conf.DataBase)
				createErr := createDatabase(_db_connection, conf.DataBase)
				if createErr != nil {
					_db_connection.Close()
					_db_connection = nil
					panic(createErr)
				}
			}

			glog.V(3).Infoln("Closing master postgres database and opening configured database: ", conf.DataBase)
			_db_connection.Close()
			_db_connection = nil

			sqlUrl = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=30", conf.HostName, conf.Port, conf.User, conf.Password, conf.DataBase, conf.SslMode)
			_db_connection, dbErr = sql.Open("postgres", sqlUrl)
			if dbErr != nil {
				_db_connection.Close()
				_db_connection = nil
				panic(dbErr)
			}

			pingErr = _db_connection.Ping()
			if pingErr != nil {
				_db_connection.Close()
				_db_connection = nil
				panic(pingErr)
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

func NewPostgresStore(confs []PostgresConf, isSharding bool, shardCount int) *PostgresStore {
	pg := &PostgresStore{
		dbs:        getDbConnection(confs),
		isSharding: isSharding,
		shardCount: shardCount,
	}

	for _, db := range pg.dbs {
		if !isSharding {
			pg.shardCount = 1
		} else {
			if pg.shardCount == 0 {
				pg.shardCount = default_maxTableNums
			}
		}
		for i := 0; i < pg.shardCount; i++ {
			if err := pg.createTables(db, tableName, i); err != nil {
				fmt.Printf("create table failed %v", err)
			}
		}
	}

	return pg
}

func (s *PostgresStore) hash(fullFileName string) (instance_offset, table_postfix int) {
	hash_value := crc32.ChecksumIEEE([]byte(fullFileName))
	instance_offset = int(hash_value) % len(s.dbs)
	table_postfix = int(hash_value) % s.shardCount
	return
}

func (s *PostgresStore) parseFilerMappingInfo(path string) (instanceId int, tableFullName string, err error) {
	instance_offset, table_postfix := s.hash(path)
	instanceId = instance_offset
	if s.isSharding {
		tableFullName = fmt.Sprintf("%s_%04d", tableName, table_postfix)
	} else {
		tableFullName = tableName
	}
	return
}

func (s *PostgresStore) Get(fullFilePath string) (fid string, err error) {
	instance_offset, tableFullName, err := s.parseFilerMappingInfo(fullFilePath)
	if err != nil {
		return "", fmt.Errorf("PostgresStore Get operation can not parse file path %s: err is %v", fullFilePath, err)
	}
	fid, err = s.query(fullFilePath, s.dbs[instance_offset], tableFullName)

	return fid, err
}

func (s *PostgresStore) Put(fullFilePath string, fid string) (err error) {
	var tableFullName string

	instance_offset, tableFullName, err := s.parseFilerMappingInfo(fullFilePath)
	if err != nil {
		return fmt.Errorf("PostgresStore Put operation can not parse file path %s: err is %v", fullFilePath, err)
	}
	var old_fid string
	if old_fid, err = s.query(fullFilePath, s.dbs[instance_offset], tableFullName); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("PostgresStore Put operation failed when querying path %s: err is %v", fullFilePath, err)
	} else {
		if len(old_fid) == 0 {
			err = s.insert(fullFilePath, fid, s.dbs[instance_offset], tableFullName)
			if err != nil {
				return fmt.Errorf("PostgresStore Put operation failed when inserting path %s with fid %s : err is %v", fullFilePath, fid, err)
			}
		} else {
			err = s.update(fullFilePath, fid, s.dbs[instance_offset], tableFullName)
			if err != nil {
				return fmt.Errorf("PostgresStore Put operation failed when updating path %s with fid %s : err is %v", fullFilePath, fid, err)
			}
		}
	}
	return
}

func (s *PostgresStore) Delete(fullFilePath string) (err error) {
	var fid string
	instance_offset, tableFullName, err := s.parseFilerMappingInfo(fullFilePath)
	if err != nil {
		return fmt.Errorf("PostgresStore Delete operation can not parse file path %s: err is %v", fullFilePath, err)
	}
	if fid, err = s.query(fullFilePath, s.dbs[instance_offset], tableFullName); err != nil {
		return fmt.Errorf("PostgresStore Delete operation failed when querying path %s: err is %v", fullFilePath, err)
	} else if fid == "" {
		return nil
	}
	if err = s.delete(fullFilePath, s.dbs[instance_offset], tableFullName); err != nil {
		return fmt.Errorf("PostgresStore Delete operation failed when deleting path %s: err is %v", fullFilePath, err)
	} else {
		return nil
	}
}

func (s *PostgresStore) Close() {
	for _, db := range s.dbs {
		db.Close()
	}
}

var createTable = `
CREATE TABLE IF NOT EXISTS %s (
  id BIGSERIAL NOT NULL,
  uriPath VARCHAR(1024) NOT NULL DEFAULT '',
  fid VARCHAR(36) NOT NULL DEFAULT '',
  createTime BIGINT NOT NULL DEFAULT 0,
  updateTime BIGINT NOT NULL DEFAULT 0,
  remark VARCHAR(20) NOT NULL DEFAULT '',
  status SMALLINT NOT NULL DEFAULT '1',
  PRIMARY KEY (id),
  CONSTRAINT %s_index_uriPath UNIQUE (uriPath)
);
`

func (s *PostgresStore) createTables(db *sql.DB, tableName string, postfix int) error {
	var realTableName string
	if s.isSharding {
		realTableName = fmt.Sprintf("%s_%04d", tableName, postfix)
	} else {
		realTableName = tableName
	}

	glog.V(3).Infoln("Creating postgres table if it doesn't exist: ", realTableName)

	sqlCreate := fmt.Sprintf(createTable, realTableName, realTableName)

	stmt, err := db.Prepare(sqlCreate)
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

func (s *PostgresStore) query(uriPath string, db *sql.DB, tableName string) (string, error) {
	sqlStatement := fmt.Sprintf("SELECT fid FROM %s WHERE uriPath=$1", tableName)

	row := db.QueryRow(sqlStatement, uriPath)
	var fid string
	err := row.Scan(&fid)

	glog.V(3).Infof("Postgres query -- looking up path '%s' and found id '%s' ", uriPath, fid)

	if err != nil {
		return "", err
	}
	return fid, nil
}

func (s *PostgresStore) update(uriPath string, fid string, db *sql.DB, tableName string) error {
	sqlStatement := fmt.Sprintf("UPDATE %s SET fid=$1, updateTime=$2 WHERE uriPath=$3", tableName)

	glog.V(3).Infof("Postgres query -- updating path '%s' with id '%s'", uriPath, fid)

	res, err := db.Exec(sqlStatement, fid, time.Now().Unix(), uriPath)
	if err != nil {
		return err
	}

	_, err = res.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}

func (s *PostgresStore) insert(uriPath string, fid string, db *sql.DB, tableName string) error {
	sqlStatement := fmt.Sprintf("INSERT INTO %s (uriPath,fid,createTime) VALUES($1, $2, $3)", tableName)

	glog.V(3).Infof("Postgres query -- inserting path '%s' with id '%s'", uriPath, fid)

	res, err := db.Exec(sqlStatement, uriPath, fid, time.Now().Unix())

	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if rows != 1 {
		return fmt.Errorf("Postgres insert -- rows affected = %d. Expecting 1", rows)
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *PostgresStore) delete(uriPath string, db *sql.DB, tableName string) error {
	sqlStatement := fmt.Sprintf("DELETE FROM %s WHERE uriPath=$1", tableName)

	glog.V(3).Infof("Postgres query -- deleting path '%s'", uriPath)

	res, err := db.Exec(sqlStatement, uriPath)
	if err != nil {
		return err
	}

	_, err = res.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}
