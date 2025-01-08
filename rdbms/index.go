package rdbms

import (
	"fmt"
	"strings"
)

var (
	mainDataSourceId = ""
	dataSourceMap    = make(map[string]IDataSource)
)

func NewDataSource(id string, jdbc_url string, statements []string, tables []ITable) (IDataSource, error) {
	if id == "" || jdbc_url == "" {
		return nil, fmt.Errorf("id or jdbc_url is empty")
	}
	jdbcUrl, err := parse_jdbc_url(jdbc_url)
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
	switch jdbcUrl.Driver {
	case "sqlite":
		ds, err = newSqliteDataSource(id, jdbcUrl.Host, statements)
		if err != nil {
			return nil, err
		}
		dataSourceMap[id] = ds
		break
	default:
		return nil, fmt.Errorf("unsupported driver: %s", jdbcUrl.Driver)
	}

	if len(tables) > 0 {
		ds.ScanTable(tables...)
	}
	if mainDataSourceId == "" && !strings.HasPrefix(id, "_") {
		mainDataSourceId = id
	}
	return ds, nil
}

func DataSource(id string) IDataSource {
	return dataSourceMap[id]
}

func Main() IDao {
	if mainDataSourceId == "" {
		return nil
	}
	return Dao(mainDataSourceId)
}

func Dao(id string) IDao {

	ds, exist := dataSourceMap[id]
	if !exist {
		return nil
	}
	return ds.NewDao()
}

func Close() {
	for _, ds := range dataSourceMap {
		ds.Close()
	}
	dataSourceMap = make(map[string]IDataSource)
}
