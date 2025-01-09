package rdbms

import (
	"fmt"
	"strings"
)

var (
	mainDataSourceId = ""
	dataSourceMap    = make(map[string]IDataSource)
)

func NewDataSource(id string, db_url string, statements []string, tables []ITable) (IDataSource, error) {
	if id == "" || db_url == "" {
		return nil, fmt.Errorf("id or url is empty")
	}
	dbUrl, err := parse_db_url(db_url)
	if err != nil {
		return nil, err
	}

	if dataSourceMap[id] != nil {
		ds := dataSourceMap[id]
		ds.Close()
		ds = nil
		delete(dataSourceMap, id)
	}

	var ds IDataSource
	switch dbUrl.Driver {
	case "sqlite":
		ds, err = newSqliteDataSource(id, dbUrl.Host, statements)
		if err != nil {
			return nil, err
		}
		dataSourceMap[id] = ds
		break
	default:
		return nil, fmt.Errorf("unsupported driver: %s", dbUrl.Driver)
	}

	if len(tables) > 0 {
		ds.ScanTable(tables...)
	}
	if mainDataSourceId == "" && !strings.HasPrefix(id, "_") {
		mainDataSourceId = id
	}
	return ds, nil
}

func GetDataSource(id string) IDataSource {
	if id == "" {
		id = mainDataSourceId
	}
	return dataSourceMap[id]
}

func Close() {
	for _, ds := range dataSourceMap {
		ds.Close()
	}
	dataSourceMap = make(map[string]IDataSource)
}
