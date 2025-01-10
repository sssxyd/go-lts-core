package rdbms

import (
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"sync"
)

type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

func scan_table(tableSpecs *sync.Map, models ...ITable) {
	for _, model := range models {
		t := reflect.TypeOf(model)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		tableName := model.TableName()
		primaryInt64Key := model.PrimaryInt64Key()
		deleteInt64Key := model.DeleteInt64Key()
		autoUpdateDBTags := make(map[string]bool)
		for _, dbTag := range model.AutoUpdateKeys() {
			autoUpdateDBTags[dbTag] = true
		}

		dbTags := []string{}
		fileNameDBTags := make(map[string]string)
		dbTagFieldNames := make(map[string]string)
		dbTagFieldIndexes := make(map[string]int)
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			dbTag := field.Tag.Get("db")
			if dbTag == "" {
				continue
			}
			dbTags = append(dbTags, dbTag)
			fileNameDBTags[field.Name] = dbTag
			dbTagFieldNames[dbTag] = field.Name
			dbTagFieldIndexes[dbTag] = i
		}

		ts := &TableSpec{
			tableName:         tableName,
			primaryInt64Key:   primaryInt64Key,
			deleteInt64Key:    deleteInt64Key,
			dbTags:            dbTags,
			autoUpdateDBTags:  autoUpdateDBTags,
			fieldNameDBTags:   fileNameDBTags,
			dbTagFieldNames:   dbTagFieldNames,
			dbTagFieldIndexes: dbTagFieldIndexes,
		}
		tableSpecs.Store(tableName, ts)
	}
}

func SqlToParams(inputs ...interface{}) []interface{} {
	var result []interface{}
	for _, input := range inputs {
		// 利用反射判断输入是否为切片
		reflectedInput := reflect.ValueOf(input)
		if reflectedInput.Kind() == reflect.Slice {
			// 遍历切片，将元素逐一添加到结果切片
			for i := 0; i < reflectedInput.Len(); i++ {
				result = append(result, reflectedInput.Index(i).Interface())
			}
		} else {
			// 非切片类型直接添加到结果切片
			result = append(result, input)
		}
	}
	return result
}

func ModelToTables(models ...interface{}) []ITable {
	var iTables []ITable
	for _, model := range models {
		if iTable, ok := model.(ITable); ok {
			iTables = append(iTables, iTable)
		}
	}
	return iTables
}

func SqlInValues(size int) string {
	placeholders := make([]string, size)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return "(" + strings.Join(placeholders, ",") + ")"
}

func parse_db_url(db_url string) (*DBUrl, error) {

	trimmedURL := strings.TrimSpace(db_url)

	// Split the URL into driver and the actual connection string
	parts := strings.SplitN(trimmedURL, ":", 2)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid JDBC URL: missing driver or connection string")
	}

	driver := parts[0]
	connectionString := parts[1]

	// Special handling for SQLite (file-based URL)
	if driver == "sqlite" {
		return &DBUrl{
			Driver: driver,
			Host:   connectionString, // SQLite uses the connection string as the file path
		}, nil
	}

	// Parse the connection string as a URL
	u, err := url.Parse(connectionString)
	if err != nil {
		return nil, fmt.Errorf("invalid JDBC URL: %v", err)
	}

	// Extract user info
	var username, password string
	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}

	// Extract query parameters
	params := make(map[string]string)
	for key, values := range u.Query() {
		if len(values) > 0 {
			params[key] = values[0]
		}
	}

	return &DBUrl{
		Driver:   driver,
		Host:     u.Hostname(),
		Port:     u.Port(),
		Database: strings.TrimPrefix(u.Path, "/"), // Remove leading slash from the path
		Username: username,
		Password: password,
		Params:   params,
	}, nil
}
