package rdbms

import "github.com/jmoiron/sqlx"

type ITable interface {
	TableName() string
	PrimaryInt64Key() string
	DeleteInt64Key() string
}

type PageData struct {
	CurrentPage int64         `json:"page"`
	PageSize    int64         `json:"size"`
	TotalPage   int64         `json:"total"`
	TotalCount  int64         `json:"count"`
	PageData    []interface{} `json:"data"`
}

// 通用任务结构
type SqlTask struct {
	SQL       string          // SQL 语句
	Args      []interface{}   // SQL 参数
	BatchArgs [][]interface{} // 批量参数
	Result    chan SqlResult  // 返回结果通道
}

func (task *SqlTask) Close() {
	close(task.Result)
}

// 通用结果结构
type SqlResult struct {
	LastInsertID []int64 // INSERT 操作的最后插入 ID
	RowsAffected int64   // UPDATE/DELETE 操作的受影响行数
	Err          error   // 错误信息
}

type IDataSource interface {
	Id() string
	Type() string
	Host() string
	Port() int
	Database() string
	Username() string
	Password() string

	ScanTable(models ...ITable)
	GetTableSpec(tableName string) *TableSpec

	NewDao() IDao
	Close() error
}

type IDao interface {
	DataSourceId() string
	Rollback() error
	Commit() error
	Close() error
	Create(statement string) error

	TableInsert(models ...ITable) ([]int64, error)
	TableUpdate(models ...ITable) (int64, error)
	TableDelete(tableName string, ids ...int64) (int64, error)
	TableGet(emptyTableModel interface{}, id int64) error
	TableSelect(emptyTableSlice interface{}, ids ...int64) error

	Conn() *sqlx.DB
}

type JdbcUrl struct {
	Driver   string            // JDBC driver name (e.g., sqlite, mysql)
	Host     string            // Hostname or file path (for sqlite)
	Port     string            // Port number (optional)
	Database string            // Database name
	Username string            // Username (optional)
	Password string            // Password (optional)
	Params   map[string]string // Additional query parameters
}
