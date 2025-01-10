package rdbms

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

type TableSpec struct {
	tableName         string            // 表名
	primaryInt64Key   string            // 主键字段
	deleteInt64Key    string            // 逻辑删除字段
	dbTags            []string          // db tags in order
	autoUpdateDBTags  map[string]bool   // 自动更新字段
	fieldNameDBTags   map[string]string // key: field name, value: db tag
	dbTagFieldNames   map[string]string // key: db tag, value: field name
	dbTagFieldIndexes map[string]int    // key: db tag, value: field index
	selectSQL         string            // 查询 SQL 语句
	insertSQL         string            // 插入 SQL 语句
	updateSQL         string            // 更新 SQL 语句
	deleteSQL         string            // 删除 SQL 语句
}

func (ts *TableSpec) TableName() string {
	return ts.tableName
}

func (ts *TableSpec) PrimaryInt64Key() string {
	return ts.primaryInt64Key
}

func (ts *TableSpec) DeleteInt64Key() string {
	return ts.deleteInt64Key
}

func (ts *TableSpec) DBTags() []string {
	return ts.dbTags
}

func (ts *TableSpec) IsLogicDelete() bool {
	return ts.deleteInt64Key != ""
}

func (ts *TableSpec) GetFieldIndex(dbTag string) (int, bool) {
	index, ok := ts.dbTagFieldIndexes[dbTag]
	return index, ok
}

func (ts *TableSpec) getInsertSql() string {
	if ts.insertSQL != "" {
		return ts.insertSQL
	}
	ts.insertSQL = generateInsertQueryFromTableSpec(ts)
	return ts.insertSQL
}

func (ts *TableSpec) getUpdateSql() string {
	if ts.updateSQL != "" {
		return ts.updateSQL
	}
	ts.updateSQL = generateUpdateQueryFromTableSpec(ts)
	return ts.updateSQL
}

func (ts *TableSpec) getDeleteSql(size int) string {
	if size == 1 {
		if ts.deleteSQL != "" {
			return ts.deleteSQL
		}
		ts.deleteSQL = generateDeleteQueryFromTableSpec(ts, 1)
		return ts.deleteSQL
	}
	return generateDeleteQueryFromTableSpec(ts, size)
}

func (ts *TableSpec) getSelectSql(size int) string {
	if size == 1 {
		if ts.selectSQL != "" {
			return ts.selectSQL
		}
		ts.selectSQL = generateSelectQueryFromTableSpec(ts, 1)
		return ts.selectSQL
	}
	return generateSelectQueryFromTableSpec(ts, size)
}

func (ts *TableSpec) extractInsertUpdateValues(model ITable) ([]interface{}, error) {
	v := reflect.ValueOf(model)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model is not a struct")
	}
	values := make([]interface{}, 0, len(ts.dbTags))
	for _, dbTag := range ts.dbTags {
		if dbTag == ts.primaryInt64Key {
			continue
		}
		fieldIndex, ok := ts.GetFieldIndex(dbTag)
		if !ok {
			return nil, fmt.Errorf("db tag %s not found in table spec", dbTag)
		}
		values = append(values, v.Field(fieldIndex).Interface())
	}
	return values, nil
}

func (ts *TableSpec) getModelId(model ITable) int64 {
	v := reflect.ValueOf(model)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return 0
	}
	fieldIndex, ok := ts.GetFieldIndex(ts.primaryInt64Key)
	if !ok {
		return 0
	}
	return v.Field(fieldIndex).Int()
}

func (ts *TableSpec) getModelIds(models []ITable) []int64 {
	ids := make([]int64, 0, len(models))
	for _, model := range models {
		ids = append(ids, ts.getModelId(model))
	}
	return ids
}

func (ts *TableSpec) setModelId(model ITable, id int64) {
	v := reflect.ValueOf(model)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return
	}
	fieldIndex, ok := ts.GetFieldIndex(ts.primaryInt64Key)
	if !ok {
		return
	}
	v.Field(fieldIndex).SetInt(id)
}

func (ts *TableSpec) UnMap(model *ITable, value map[string]interface{}) error {
	v := reflect.ValueOf(model)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("model is not a struct")
	}
	for key, fieldValue := range value {
		// key is db tag
		if fieldIndex, ok := ts.GetFieldIndex(key); ok {
			v.Field(fieldIndex).Set(reflect.ValueOf(fieldValue))
			continue
		}
		// key is filed name
		dbTag, ok := ts.fieldNameDBTags[key]
		if ok {
			fieldIndex, ok := ts.GetFieldIndex(dbTag)
			if ok {
				v.Field(fieldIndex).Set(reflect.ValueOf(fieldValue))
			}
		}
	}
	return nil
}

func generateInsertQueryFromTableSpec(ts *TableSpec) string {
	columns := make([]string, 0, len(ts.dbTags))
	values := make([]string, 0, len(ts.dbTags))
	for _, dbTag := range ts.dbTags {
		if dbTag == ts.primaryInt64Key {
			continue
		}
		columns = append(columns, dbTag)
		if dbTag == ts.deleteInt64Key {
			values = append(values, "0")
		} else {
			values = append(values, "?")
		}
	}
	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		ts.tableName,
		strings.Join(columns, ","),
		strings.Join(values, ","),
	)
	return sql
}

func generateUpdateQueryFromTableSpec(ts *TableSpec) string {
	columns := make([]string, 0, len(ts.dbTags))
	for _, dbTag := range ts.dbTags {
		if dbTag == ts.primaryInt64Key || dbTag == ts.deleteInt64Key || ts.autoUpdateDBTags[dbTag] {
			continue
		}
		columns = append(columns, fmt.Sprintf("%s = ?", dbTag))
	}
	sql := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s = ?",
		ts.tableName,
		strings.Join(columns, ","),
		ts.primaryInt64Key,
	)
	return sql
}

func generateDeleteQueryFromTableSpec(ts *TableSpec, size int) string {
	var sql string
	if size == 1 {
		if ts.IsLogicDelete() {
			sql = fmt.Sprintf(
				"UPDATE %s SET %s = %d WHERE %s = ?",
				ts.tableName,
				ts.deleteInt64Key,
				time.Now().Unix(),
				ts.primaryInt64Key,
			)
		} else {
			sql = fmt.Sprintf(
				"DELETE FROM %s WHERE %s = ?",
				ts.tableName,
				ts.primaryInt64Key,
			)
		}
	} else {
		placeholder := make([]string, 0, size)
		for i := 0; i < size; i++ {
			placeholder = append(placeholder, "?")
		}
		if ts.IsLogicDelete() {
			sql = fmt.Sprintf(
				"UPDATE %s SET %s = %d WHERE %s in (%s)",
				ts.tableName,
				ts.deleteInt64Key,
				time.Now().Unix(),
				ts.primaryInt64Key,
				strings.Join(placeholder, ","),
			)
		} else {
			sql = fmt.Sprintf(
				"DELETE FROM %s WHERE %s in (%s)",
				ts.tableName,
				ts.primaryInt64Key,
				strings.Join(placeholder, ","),
			)
		}
	}
	return sql
}

func generateSelectQueryFromTableSpec(ts *TableSpec, size int) string {
	columns := make([]string, 0, len(ts.dbTags))
	for _, dbTag := range ts.dbTags {
		columns = append(columns, dbTag)
	}
	var sql string
	if size == 1 {
		sql = fmt.Sprintf(
			"SELECT %s FROM %s WHERE %s = ?",
			strings.Join(columns, ","),
			ts.tableName,
			ts.primaryInt64Key,
		)
	} else {
		placeholder := make([]string, 0, size)
		for i := 0; i < size; i++ {
			placeholder = append(placeholder, "?")
		}
		sql = fmt.Sprintf(
			"SELECT %s FROM %s WHERE %s in (%s)",
			strings.Join(columns, ","),
			ts.tableName,
			ts.primaryInt64Key,
			strings.Join(placeholder, ","),
		)
	}
	if ts.IsLogicDelete() {
		sql = fmt.Sprintf("%s AND %s = 0", sql, ts.deleteInt64Key)
	}
	return sql
}
