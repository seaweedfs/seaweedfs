package postgres_store

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"

	_ "github.com/lib/pq"
	_ "path/filepath"
	"strings"
)

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

func getDbConnection(conf PostgresConf) *sql.DB {
	_init_db.Do(func() {

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
	})
	return _db_connection
}

var createDirectoryTable = `

CREATE TABLE IF NOT EXISTS %s (
  id BIGSERIAL NOT NULL,
  directoryRoot VARCHAR(1024) NOT NULL DEFAULT '',
  directoryName VARCHAR(1024) NOT NULL DEFAULT '',
  CONSTRAINT unique_directory UNIQUE (directoryRoot, directoryName)
);
`

var createFileTable = `

CREATE TABLE IF NOT EXISTS %s (
  id BIGSERIAL NOT NULL,
  directoryPart VARCHAR(1024) NOT NULL DEFAULT '',
  filePart VARCHAR(1024) NOT NULL DEFAULT '',
  fid VARCHAR(36) NOT NULL DEFAULT '',
  createTime BIGINT NOT NULL DEFAULT 0,
  updateTime BIGINT NOT NULL DEFAULT 0,
  remark VARCHAR(20) NOT NULL DEFAULT '',
  status SMALLINT NOT NULL DEFAULT '1',
  PRIMARY KEY (id),
  CONSTRAINT %s_unique_file UNIQUE (directoryPart, filePart)
);
`

func (s *PostgresStore) createDirectoriesTable() error {
	glog.V(3).Infoln("Creating postgres table if it doesn't exist: ", directoriesTableName)

	sqlCreate := fmt.Sprintf(createDirectoryTable, directoriesTableName)

	stmt, err := s.db.Prepare(sqlCreate)
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

func (s *PostgresStore) createFilesTable() error {

	glog.V(3).Infoln("Creating postgres table if it doesn't exist: ", filesTableName)

	sqlCreate := fmt.Sprintf(createFileTable, filesTableName, filesTableName)

	stmt, err := s.db.Prepare(sqlCreate)
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

func (s *PostgresStore) query(uriPath string) (string, error) {
	directoryPart, filePart := filepath.Split(uriPath)
	sqlStatement := fmt.Sprintf("SELECT fid FROM %s WHERE directoryPart=$1 AND filePart=$2", filesTableName)

	row := s.db.QueryRow(sqlStatement, directoryPart, filePart)
	var fid string
	err := row.Scan(&fid)

	glog.V(3).Infof("Postgres query -- looking up path '%s' and found id '%s' ", uriPath, fid)

	if err != nil {
		return "", err
	}
	return fid, nil
}

func (s *PostgresStore) update(uriPath string, fid string) error {
	directoryPart, filePart := filepath.Split(uriPath)
	sqlStatement := fmt.Sprintf("UPDATE %s SET fid=$1, updateTime=$2 WHERE directoryPart=$3 AND filePart=$4", filesTableName)

	glog.V(3).Infof("Postgres query -- updating path '%s' with id '%s'", uriPath, fid)

	res, err := s.db.Exec(sqlStatement, fid, time.Now().Unix(), directoryPart, filePart)
	if err != nil {
		return err
	}

	_, err = res.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}

func (s *PostgresStore) insert(uriPath string, fid string) error {
	directoryPart, filePart := filepath.Split(uriPath)

	existingId, _, _ := s.lookupDirectory(directoryPart)
	if existingId == 0 {
		s.recursiveInsertDirectory(directoryPart)
	}

	sqlStatement := fmt.Sprintf("INSERT INTO %s (directoryPart,filePart,fid,createTime) VALUES($1, $2, $3, $4)", filesTableName)
	glog.V(3).Infof("Postgres query -- inserting path '%s' with id '%s'", uriPath, fid)

	res, err := s.db.Exec(sqlStatement, directoryPart, filePart, fid, time.Now().Unix())

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

func (s *PostgresStore) recursiveInsertDirectory(dirPath string) {
	pathParts := strings.Split(dirPath, "/")

	var workingPath string = "/"
	for _, part := range pathParts {
		if part == "" {
			continue
		}
		workingPath += (part + "/")
		existingId, _, _ := s.lookupDirectory(workingPath)
		if existingId == 0 {
			s.insertDirectory(workingPath)
		}
	}
}

func (s *PostgresStore) insertDirectory(dirPath string) {
	pathParts := strings.Split(dirPath, "/")

	directoryRoot := "/"
	directoryName := ""
	if len(pathParts) > 1 {
		directoryRoot = strings.Join(pathParts[0:len(pathParts)-2], "/") + "/"
		directoryName = strings.Join(pathParts[len(pathParts)-2:], "/")
	} else if len(pathParts) == 1 {
		directoryRoot = "/"
		directoryName = pathParts[0] + "/"
	}
	sqlInsertDirectoryStatement := fmt.Sprintf("INSERT INTO %s (directoryroot, directoryname)  "+
		"SELECT $1, $2 WHERE NOT EXISTS ( SELECT id FROM %s WHERE directoryroot=$3 AND directoryname=$4 )",
		directoriesTableName, directoriesTableName)

	glog.V(4).Infof("Postgres query -- Inserting directory (if it doesn't exist) - root = %s, name = %s",
		directoryRoot, directoryName)

	_, err := s.db.Exec(sqlInsertDirectoryStatement, directoryRoot, directoryName, directoryRoot, directoryName)
	if err != nil {
		glog.V(0).Infof("Postgres query -- Error inserting directory - root = %s, name = %s: %s",
			directoryRoot, directoryName, err)
	}
}

func (s *PostgresStore) delete(uriPath string) error {
	directoryPart, filePart := filepath.Split(uriPath)
	sqlStatement := fmt.Sprintf("DELETE FROM %s WHERE directoryPart=$1 AND filePart=$2", filesTableName)

	glog.V(3).Infof("Postgres query -- deleting path '%s'", uriPath)

	res, err := s.db.Exec(sqlStatement, directoryPart, filePart)
	if err != nil {
		return err
	}

	_, err = res.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}

func (s *PostgresStore) lookupDirectory(dirPath string) (filer.DirectoryId, string, error) {
	directoryRoot, directoryName := s.mySplitPath(dirPath)

	sqlStatement := fmt.Sprintf("SELECT id, directoryroot, directoryname FROM %s WHERE directoryRoot=$1 AND directoryName=$2", directoriesTableName)

	row := s.db.QueryRow(sqlStatement, directoryRoot, directoryName)
	var id filer.DirectoryId
	var dirRoot string
	var dirName string
	err := row.Scan(&id, &dirRoot, &dirName)

	glog.V(3).Infof("Postgres lookupDirectory -- looking up directory '%s' and found id '%d', root '%s', name '%s' ", dirPath, id, dirRoot, dirName)

	if err != nil {
		return 0, "", err
	}
	return id, filepath.Join(dirRoot, dirName), err
}

func (s *PostgresStore) findDirectories(dirPath string, limit int) (dirs []filer.DirectoryEntry, err error) {
	sqlStatement := fmt.Sprintf("SELECT id, directoryroot, directoryname FROM %s WHERE directoryRoot=$1 AND directoryName != '' ORDER BY id LIMIT $2", directoriesTableName)
	rows, err := s.db.Query(sqlStatement, dirPath, limit)

	if err != nil {
		glog.V(0).Infof("Postgres findDirectories error: %s", err)
	}

	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var id filer.DirectoryId
			var directoryRoot string
			var directoryName string

			scanErr := rows.Scan(&id, &directoryRoot, &directoryName)
			if scanErr != nil {
				err = scanErr
			}
			dirs = append(dirs, filer.DirectoryEntry{Name: (directoryName), Id: id})
		}
	}
	return
}

func (s *PostgresStore) safeToDeleteDirectory(dirPath string, recursive bool) bool {
	if recursive {
		return true
	}
	sqlStatement := fmt.Sprintf("SELECT id FROM %s WHERE directoryRoot LIKE $1 LIMIT 1", directoriesTableName)
	row := s.db.QueryRow(sqlStatement, dirPath+"%")

	var id filer.DirectoryId
	err := row.Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			return true
		}
	}
	return false
}

func (s *PostgresStore) mySplitPath(dirPath string) (directoryRoot string, directoryName string) {
	pathParts := strings.Split(dirPath, "/")
	directoryRoot = "/"
	directoryName = ""
	if len(pathParts) > 1 {
		directoryRoot = strings.Join(pathParts[0:len(pathParts)-2], "/") + "/"
		directoryName = strings.Join(pathParts[len(pathParts)-2:], "/")
	} else if len(pathParts) == 1 {
		directoryRoot = "/"
		directoryName = pathParts[0] + "/"
	}
	return directoryRoot, directoryName
}

func (s *PostgresStore) deleteDirectory(dirPath string, recursive bool) (err error) {
	directoryRoot, directoryName := s.mySplitPath(dirPath)

	// delete files
	sqlStatement := fmt.Sprintf("DELETE FROM %s WHERE directorypart=$1", filesTableName)
	_, err = s.db.Exec(sqlStatement, dirPath)
	if err != nil {
		return err
	}

	// delete specific directory if it is empty or recursive delete was requested
	safeToDelete := s.safeToDeleteDirectory(dirPath, recursive)
	if safeToDelete {
		sqlStatement = fmt.Sprintf("DELETE FROM %s WHERE directoryRoot=$1 AND directoryName=$2", directoriesTableName)
		_, err = s.db.Exec(sqlStatement, directoryRoot, directoryName)
		if err != nil {
			return err
		}
	}

	if recursive {
		// delete descendant files
		sqlStatement = fmt.Sprintf("DELETE FROM %s WHERE directorypart LIKE $1", filesTableName)
		_, err = s.db.Exec(sqlStatement, dirPath+"%")
		if err != nil {
			return err
		}

		// delete descendant directories
		sqlStatement = fmt.Sprintf("DELETE FROM %s WHERE directoryRoot LIKE $1", directoriesTableName)
		_, err = s.db.Exec(sqlStatement, dirPath+"%")
		if err != nil {
			return err
		}
	}

	return err
}

func (s *PostgresStore) findFiles(dirPath string, lastFileName string, limit int) (files []filer.FileEntry, err error) {
	var rows *sql.Rows = nil

	if lastFileName == "" {
		sqlStatement :=
			fmt.Sprintf("SELECT fid, directorypart, filepart FROM %s WHERE directorypart=$1 ORDER BY id LIMIT $2", filesTableName)
		rows, err = s.db.Query(sqlStatement, dirPath, limit)
	} else {
		sqlStatement :=
			fmt.Sprintf("SELECT fid, directorypart, filepart FROM %s WHERE directorypart=$1 "+
				"AND id > (SELECT id FROM %s WHERE directoryPart=$2 AND filepart=$3)  ORDER BY id LIMIT $4",
				filesTableName, filesTableName)
		_, lastFileNameName := filepath.Split(lastFileName)
		rows, err = s.db.Query(sqlStatement, dirPath, dirPath, lastFileNameName, limit)
	}

	if err != nil {
		glog.V(0).Infof("Postgres find files error: %s", err)
	}

	if rows != nil {
		defer rows.Close()

		for rows.Next() {
			var fid filer.FileId
			var directoryPart string
			var filePart string

			scanErr := rows.Scan(&fid, &directoryPart, &filePart)
			if scanErr != nil {
				err = scanErr
			}

			files = append(files, filer.FileEntry{Name: filepath.Join(directoryPart, filePart), Id: fid})
			if len(files) >= limit {
				break
			}
		}
	}

	glog.V(3).Infof("Postgres findFiles -- looking up files under '%s' and found %d files. Limit=%d, lastFileName=%s",
		dirPath, len(files), limit, lastFileName)

	return files, err
}
