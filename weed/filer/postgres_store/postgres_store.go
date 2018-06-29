package postgres_store

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"

	_ "github.com/lib/pq"
	_ "path/filepath"
	"path/filepath"
)

const (
	default_maxIdleConnections = 100
	default_maxOpenConnections = 50
	filesTableName             = "files"
	directoriesTableName       = "directories"
)

var (
	_init_db       sync.Once
	_db_connection *sql.DB
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

type PostgresStore struct {
	db       *sql.DB
	server   string
	user     string
	password string
}

func (s *PostgresStore) CreateFile(fullFilePath string, fid string) (err error) {

	var old_fid string
	if old_fid, err = s.query(fullFilePath); err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("PostgresStore Put operation failed when querying path %s: err is %v", fullFilePath, err)
	} else {
		if len(old_fid) == 0 {
			err = s.insert(fullFilePath, fid)
			if err != nil {
				return fmt.Errorf("PostgresStore Put operation failed when inserting path %s with fid %s : err is %v", fullFilePath, fid, err)
			}
		} else {
			err = s.update(fullFilePath, fid)
			if err != nil {
				return fmt.Errorf("PostgresStore Put operation failed when updating path %s with fid %s : err is %v", fullFilePath, fid, err)
			}
		}
	}
	return

}

func (s *PostgresStore) FindFile(fullFilePath string) (fid string, err error) {

	if err != nil {
		return "", fmt.Errorf("PostgresStore Get operation can not parse file path %s: err is %v", fullFilePath, err)
	}
	fid, err = s.query(fullFilePath)

	return fid, err
}

func (s *PostgresStore) LookupDirectoryEntry(dirPath string, name string) (found bool, fileId string, err error) {
	fullPath := filepath.Join(dirPath, name)
	if fileId, err = s.FindFile(fullPath); err == nil {
		return true, fileId, nil
	}
	if _, _, err := s.lookupDirectory(fullPath); err == nil {
		return true, "", err
	}
	return false, "", err
}

func (s *PostgresStore) DeleteFile(fullFilePath string) (fid string, err error) {
	if err != nil {
		return "", fmt.Errorf("PostgresStore Delete operation can not parse file path %s: err is %v", fullFilePath, err)
	}
	if fid, err = s.query(fullFilePath); err != nil {
		return "", fmt.Errorf("PostgresStore Delete operation failed when querying path %s: err is %v", fullFilePath, err)
	} else if fid == "" {
		return "", nil
	}
	if err = s.delete(fullFilePath); err != nil {
		return "", fmt.Errorf("PostgresStore Delete operation failed when deleting path %s: err is %v", fullFilePath, err)
	} else {
		return "", nil
	}
}

func (s *PostgresStore) ListDirectories(dirPath string) (dirs []filer.DirectoryName, err error) {

	dirs, err = s.findDirectories(dirPath, 1000)

	glog.V(3).Infof("Postgres ListDirs = found %d directories under %s", len(dirs), dirPath)

	return dirs, err
}

func (s *PostgresStore) ListFiles(dirPath string, lastFileName string, limit int) (files []filer.FileEntry, err error) {
	files, err = s.findFiles(dirPath, lastFileName, limit)
	return files, err
}

func (s *PostgresStore) DeleteDirectory(dirPath string, recursive bool) (err error) {
	err = s.deleteDirectory(dirPath, recursive)
	if err != nil {
		glog.V(0).Infof("Error in Postgres DeleteDir '%s' (recursive = '%t'): %v", dirPath, recursive, err)
	}
	return err
}

func (s *PostgresStore) Move(fromPath string, toPath string) (err error) {
	glog.V(3).Infoln("Calling posgres_store Move")
	return errors.New("Move is not yet implemented for the PostgreSQL store.")
}

//func NewPostgresStore(master string, confs []PostgresConf, isSharding bool, shardCount int) *PostgresStore {
func NewPostgresStore(master string, conf PostgresConf) *PostgresStore {
	pg := &PostgresStore{
		db: getDbConnection(conf),
	}

	pg.createDirectoriesTable()

	if err := pg.createFilesTable(); err != nil {
		fmt.Printf("create table failed %v", err)
	}

	return pg
}

func (s *PostgresStore) Close() {
	s.db.Close()
}
