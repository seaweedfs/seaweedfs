package abstract_sql

import (
	"fmt"
	"database/sql"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

type AbstractSqlStore struct {
	DB *sql.DB
}

func (store *AbstractSqlStore) InsertEntry(entry *filer2.Entry) (err error) {

	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("mysql encode %s: %s", entry.FullPath, err)
	}

	res, err := store.DB.Exec("INSERT INTO filemeta (dirhash,name,directory,meta) VALUES(?,?,?,?)",
		md5hash(dir), name, dir, meta)
	if err != nil {
		return fmt.Errorf("mysql insert %s: %s", entry.FullPath, err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("mysql insert %s but no rows affected: %s", entry.FullPath, err)
	}
	return nil
}

func (store *AbstractSqlStore) UpdateEntry(entry *filer2.Entry) (err error) {

	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("mysql encode %s: %s", entry.FullPath, err)
	}

	res, err := store.DB.Exec("UPDATE filemeta SET meta=? WHERE dirhash=? AND name=? AND directory=?",
		meta, md5hash(dir), name, dir)
	if err != nil {
		return fmt.Errorf("mysql update %s: %s", entry.FullPath, err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("mysql update %s but no rows affected: %s", entry.FullPath, err)
	}
	return nil
}

func (store *AbstractSqlStore) FindEntry(fullpath filer2.FullPath) (*filer2.Entry, error) {

	dir, name := fullpath.DirAndName()
	row := store.DB.QueryRow("SELECT meta FROM filemeta WHERE dirhash=? AND name=? AND directory=?",
		md5hash(dir), name, dir)
	var data []byte
	if err := row.Scan(&data); err != nil {
		return nil, fmt.Errorf("mysql read entry %s: %v", fullpath, err)
	}

	entry := &filer2.Entry{
		FullPath: fullpath,
	}
	if err := entry.DecodeAttributesAndChunks(data); err != nil {
		return entry, fmt.Errorf("mysql decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *AbstractSqlStore) DeleteEntry(fullpath filer2.FullPath) (*filer2.Entry, error) {

	entry, _ := store.FindEntry(fullpath)

	dir, name := fullpath.DirAndName()

	res, err := store.DB.Exec("DELETE FROM filemeta WHERE dirhash=? AND name=? AND directory=?",
		md5hash(dir), name, dir)
	if err != nil {
		return nil, fmt.Errorf("mysql delete %s: %s", fullpath, err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("mysql delete %s but no rows affected: %s", fullpath, err)
	}

	return entry, nil
}

func (store *AbstractSqlStore) ListDirectoryEntries(fullpath filer2.FullPath, startFileName string, inclusive bool, limit int) (entries []*filer2.Entry, err error) {

	sqlText := "SELECT NAME, meta FROM filemeta WHERE dirhash=? AND name>? AND directory=? LIMIT ?"
	if inclusive {
		sqlText = "SELECT NAME, meta FROM filemeta WHERE dirhash=? AND name>=? AND directory=? LIMIT ?"
	}

	rows, err := store.DB.Query(sqlText, md5hash(string(fullpath)), startFileName, string(fullpath), limit)
	if err != nil {
		return nil, fmt.Errorf("mysql list %s : %v", fullpath, err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var data []byte
		if err = rows.Scan(&name, &data); err != nil {
			glog.V(0).Infof("mysql scan %s : %v", fullpath, err)
			return nil, fmt.Errorf("mysql scan %s: %v", fullpath, err)
		}

		entry := &filer2.Entry{
			FullPath: filer2.NewFullPath(string(fullpath), name),
		}
		if err = entry.DecodeAttributesAndChunks(data); err != nil {
			glog.V(0).Infof("mysql scan decode %s : %v", entry.FullPath, err)
			return nil, fmt.Errorf("mysql scan decode %s : %v", entry.FullPath, err)
		}

		entries = append(entries, entry)
	}

	return entries, nil
}
