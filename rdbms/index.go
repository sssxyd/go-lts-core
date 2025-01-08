package rdbms

import (
	"fmt"
)

var (
	dataSourceMap = make(map[string]IDataSource)
)

func NewDataSource(id string, jdbc_url string, statements []string) (IDataSource, error) {
	if id == "" || jdbc_url == "" {
		return nil, fmt.Errorf("id or jdbc_url is empty")
	}
	jdbcUrl, err := parse_jdbc_url(jdbc_url)
	if err != nil {
		return nil, err
	}

	var ds IDataSource
	if dataSourceMap[id] != nil {
		ds = dataSourceMap[id]
		ds.Close()
		ds = nil
		delete(dataSourceMap, id)
	}

	switch jdbcUrl.Driver {
	case "sqlite":
		ds = newSqliteDataSource(id, jdbcUrl.Host, statements)
		dataSourceMap[id] = ds
		return ds, nil
	default:
		return nil, fmt.Errorf("unsupported driver: %s", jdbcUrl.Driver)
	}
}

func DataSource(id string) IDataSource {
	ds, ok := dataSourceMap[id]
	if ok {
		return ds
	}
	return nil
}

func Close() {
	for _, ds := range dataSourceMap {
		ds.Close()
	}
	dataSourceMap = make(map[string]IDataSource)
}
