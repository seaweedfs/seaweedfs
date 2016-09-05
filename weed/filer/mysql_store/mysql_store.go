package mysql_store

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

var maxOpen int = 100

type MysqlStore struct {
	Client   *sql.DB
	table    string
	fnameCol string
	fidCol   string
	getStmt  *sql.Stmt
	putStmt  *sql.Stmt
	delStmt  *sql.Stmt
}

func NewMysqlStore(dataSourceName string, table, fnameCol, fidCol string) (m *MysqlStore, err error) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen / 2)
	getStmt, err := db.Prepare(fmt.Sprintf("select %s from %s where %s=?", fidCol, table, fnameCol))
	if err != nil {
		return nil, err
	}
	putStmt, err := db.Prepare(fmt.Sprintf("insert into %s(%s,%s) values(?,?)", table, fnameCol, fidCol))
	if err != nil {
		return nil, err
	}
	delStmt, err := db.Prepare(fmt.Sprintf("delete from %s where %s=?", table, fnameCol))
	if err != nil {
		return nil, err
	}

	return &MysqlStore{db, table, fnameCol, fidCol, getStmt, putStmt, delStmt}, nil
}

func (s *MysqlStore) Get(fullFileName string) (fid string, err error) {
	err = s.getStmt.QueryRow(fullFileName).Scan(&fid)
	return fid, err
}

func (s *MysqlStore) Put(fullFileName string, fid string) (err error) {
	_, err = s.putStmt.Exec(fullFileName, fid)
	return err
}

func (s *MysqlStore) Delete(fullFileName string) (err error) {
	_, err = s.delStmt.Exec(fullFileName)
	return err
}

func (s *MysqlStore) Close() {
	if s.getStmt != nil {
		s.getStmt.Close()
	}
	if s.putStmt != nil {
		s.putStmt.Close()
	}
	if s.delStmt != nil {
		s.delStmt.Close()
	}
	if s.Client != nil {
		s.Client.Close()
	}
}
