package rdbms

import (
	"path/filepath"
	"sync"
)

var (
	sqliteInstance IDataSource
	sqliteOnce     sync.Once
)

func Sqlite(statements []string) IDataSource {
	sqliteOnce.Do(func() {
		root_dir := get_app_root_dir()
		db_path := filepath.Join(root_dir, "localdb", "default.db")
		sqliteInstance = newSqliteDataSource("default", db_path, statements)
	})
	return sqliteInstance
}

func Close() {
	if sqliteInstance != nil {
		sqliteInstance.Close()
		sqliteInstance = nil
	}
}
