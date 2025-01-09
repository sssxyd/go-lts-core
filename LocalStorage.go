package lts

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/sssxyd/go-lts-core/rdbms"
)

const local_storage_datasource_id = "_local_storage"
const permanent_unix_time = 4891334400 // 2125-01-01 00:00:00

type StorageModel struct {
	ID         string `db:"id"`
	StoreKey   string `db:"store_key"`
	StoreValue string `db:"store_value"`
	ExpiredAt  int64  `db:"expired_at"`
}

func (s *StorageModel) TableName() string {
	return "storage"
}

func (s *StorageModel) PrimaryInt64Key() string {
	return "id"
}

func (s *StorageModel) DeleteInt64Key() string {
	return ""
}

type LocalStorage struct {
	dao rdbms.IDao
}

func init_local_storage(storageFilePath string) *LocalStorage {
	storageFilePath = strings.ReplaceAll(storageFilePath, "\\", "/")
	statements := []string{
		`CREATE TABLE IF NOT EXISTS "storage" (
			"id"	INTEGER NOT NULL UNIQUE,
			"store_key"	TEXT NOT NULL DEFAULT "",
			"store_value"	TEXT NOT NULL DEFAULT "",
			"expired_at"	INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY("id" AUTOINCREMENT)
		);`,
		`CREATE INDEX IF NOT EXISTS "idx_storage_store_key" ON "storage" (
			"store_key"	ASC
		);`,
		`CREATE INDEX IF NOT EXISTS "idx_storage_expired_at" ON "storage" (
			"expired_at"	ASC
		);`,
	}
	tables := []rdbms.ITable{&StorageModel{}}
	ds, err := rdbms.NewDataSource(local_storage_datasource_id, fmt.Sprintf("sqlite:%s", storageFilePath), statements, tables)
	if err != nil {
		panic(err)
	}
	dao := ds.NewDao()
	instance := &LocalStorage{dao: dao}
	instance.clear_expired_items()

	return instance
}

func (l *LocalStorage) clear_expired_items() {
	sql := "SELECT id FROM storage WHERE expired_at < ?"
	rows, err := l.dao.Conn().Query(sql, time.Now().Unix())
	if err != nil {
		return
	}
	defer rows.Close()
	ids := []int64{}
	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	l.dao.TableDelete("storage", ids...)
}

func (l *LocalStorage) Remove(keys ...string) int {
	sql := "SELECT id FROM storage WHERE store_key IN " + rdbms.SqlInValues(len(keys))
	rows, err := l.dao.Conn().Query(sql, rdbms.SqlToParams(keys)...)
	if err != nil {
		return 0
	}
	defer rows.Close()
	ids := []int64{}
	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			log.Printf("LocalStorage Remove error: %v\n", err)
			continue
		}
		ids = append(ids, id)
	}
	count, err := l.dao.TableDelete("storage", ids...)
	if err != nil {
		log.Printf("LocalStorage Remove error: %v\n", err)
	}
	return int(count)
}

func (l *LocalStorage) Get(key string) string {
	store := &StorageModel{}
	var model StorageModel

	sql := "SELECT * FROM storage WHERE store_key = ? AND expired_at >= ? ORDER BY id DESC LIMIT 1"
	err := l.dao.Conn().Get(store, sql, key, time.Now().Unix())
	if err != nil {
		return ""
	}
	return model.StoreValue
}

func (l *LocalStorage) MGet(keys ...string) map[string]string {
	if len(keys) == 0 {
		return map[string]string{}
	}
	models := []StorageModel{}
	sql := "SELECT * FROM storage WHERE store_key IN " + rdbms.SqlInValues(len(keys)) + " AND expired_at >= ?"
	err := l.dao.Conn().Select(&models, sql, rdbms.SqlToParams(keys), time.Now().Unix())
	if err != nil {
		return map[string]string{}
	}
	result := map[string]string{}
	for _, model := range models {
		result[model.StoreKey] = model.StoreValue
	}
	return result
}

func (l *LocalStorage) Set(key string, value string) {
	l.SetEx(key, value, permanent_unix_time)
}

func (l *LocalStorage) SetEx(key string, value string, expiredAt int64) {
	l.Remove(key)

	if expiredAt <= 0 {
		expiredAt = permanent_unix_time
	}

	store := &StorageModel{
		StoreKey:   key,
		StoreValue: value,
		ExpiredAt:  expiredAt,
	}
	l.dao.TableInsert(store)
}

func (l *LocalStorage) MSet(data map[string]string) {
	l.MSetEx(data, permanent_unix_time)
}

func (l *LocalStorage) MSetEx(data map[string]string, expiredAt int64) {
	if len(data) == 0 {
		return
	}

	if expiredAt <= 0 {
		expiredAt = permanent_unix_time
	}

	keys := []string{}
	for key := range data {
		keys = append(keys, key)
	}
	l.Remove(keys...)

	models := []StorageModel{}
	for key, value := range data {
		store := &StorageModel{
			StoreKey:   key,
			StoreValue: value,
			ExpiredAt:  expiredAt,
		}
		models = append(models, *store)
	}
	l.dao.TableInsert(rdbms.ModelToTables(models)...)
}
