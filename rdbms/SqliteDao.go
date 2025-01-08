package rdbms

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

type SqliteDao struct {
	ds *SqliteDataSource
}

func (dao *SqliteDao) DataSourceId() string {
	return dao.ds.Id()
}

func (dao *SqliteDao) Rollback() error {
	return nil
}

func (dao *SqliteDao) Commit() error {
	return nil
}

func (dao *SqliteDao) Close() error {
	return nil
}

func (dao *SqliteDao) Create(statement string) error {
	statement = strings.TrimSpace(statement)
	if statement == "" {
		return nil
	}

	if !strings.HasPrefix(strings.ToLower(statement), "create ") {
		return fmt.Errorf("only support create statement")
	}

	task := SqlTask{
		SQL:    statement,
		Result: make(chan SqlResult, 1),
	}

	defer task.Close()
	dao.ds.doTasks <- task
	result := <-task.Result
	if result.Err != nil {
		return result.Err
	}
	return nil
}

func (dao *SqliteDao) prepare_insert_update_tasks(models []ITable, update bool) ([]SqlTask, error) {
	groups := make(map[string][]ITable)
	for _, model := range models {
		tableName := model.TableName()
		if group, ok := groups[tableName]; ok {
			groups[tableName] = append(group, model)
		} else {
			groups[tableName] = []ITable{model}
		}
	}
	tasks := make([]SqlTask, 0, len(groups))
	var err error
	defer func() {
		if err != nil {
			for _, task := range tasks {
				task.Close()
			}
		}
	}()

	// 处理各个分组
	for tableName, group := range groups {
		ts := dao.ds.GetTableSpec(tableName)
		if ts == nil {
			err = fmt.Errorf("table[%s] spec not found", tableName)
			return nil, err
		}

		// 处理单个任务或批量任务
		var task SqlTask
		if len(group) == 1 {
			args, err := ts.extractInsertUpdateValues(group[0])
			if err != nil {
				return nil, err
			}
			if update {
				pk := ts.getModelId(group[0])
				if pk <= 0 {
					return nil, fmt.Errorf("primary is zero of model %v", group[0])
				}
				task = SqlTask{
					SQL:    ts.getUpdateSql(),
					Args:   args,
					Result: make(chan SqlResult, 1),
				}
			} else {
				task = SqlTask{
					SQL:    ts.getInsertSql(),
					Args:   args,
					Result: make(chan SqlResult, 1),
				}
			}
		} else {
			batchArgs := make([][]interface{}, 0, len(group))
			for _, model := range group {
				args, err := ts.extractInsertUpdateValues(model)
				if err != nil {
					return nil, err
				}
				if update {
					pk := ts.getModelId(model)
					if pk <= 0 {
						return nil, fmt.Errorf("primary is zero of model %v", model)
					}
					args = append(args, pk)
				}
				batchArgs = append(batchArgs, args)
			}
			if update {
				task = SqlTask{
					SQL:       ts.getUpdateSql(),
					BatchArgs: batchArgs,
					Result:    make(chan SqlResult, 1),
				}
			} else {
				task = SqlTask{
					SQL:       ts.getInsertSql(),
					BatchArgs: batchArgs,
					Result:    make(chan SqlResult, 1),
				}
			}
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (dao *SqliteDao) TableInsert(models ...ITable) ([]int64, error) {
	if len(models) == 0 {
		return nil, nil
	}
	tasks, err := dao.prepare_insert_update_tasks(models, false)
	if err != nil {
		return nil, err
	}

	pks := make([]int64, 0, len(models))
	for _, task := range tasks {
		dao.ds.doTasks <- task
		result := <-task.Result
		if result.Err != nil {
			return nil, result.Err
		}
		if result.LastInsertID != nil {
			pks = append(pks, result.LastInsertID...)
		}
		task.Close()
	}
	return pks, nil
}

func (dao *SqliteDao) TableUpdate(models ...ITable) (int64, error) {
	if len(models) == 0 {
		return 0, nil
	}
	tasks, err := dao.prepare_insert_update_tasks(models, false)
	if err != nil {
		return 0, err
	}

	count := int64(0)
	for _, task := range tasks {
		dao.ds.doTasks <- task
		result := <-task.Result
		if result.Err != nil {
			return 0, result.Err
		}
		count += result.RowsAffected
		task.Close()
	}
	return count, nil
}

func (dao *SqliteDao) TableDelete(tableName string, ids ...int64) (int64, error) {
	ts := dao.ds.GetTableSpec(tableName)
	if ts == nil {
		return 0, fmt.Errorf("table[%s] spec not found", tableName)
	}
	if len(ids) == 0 {
		return 0, nil
	}

	task := SqlTask{
		SQL:    ts.getDeleteSql(len(ids)),
		Args:   SqlToParams(ids),
		Result: make(chan SqlResult, 1),
	}

	defer task.Close()

	dao.ds.doTasks <- task
	result := <-task.Result

	if result.Err != nil {
		return 0, result.Err
	}
	return result.RowsAffected, nil
}

func (dao *SqliteDao) TableGet(emptyTableModel interface{}, id int64) error {
	model, ok := emptyTableModel.(ITable)
	if !ok {
		return fmt.Errorf("emptyTableModel not implement ITable interface")
	}
	ts := dao.ds.GetTableSpec(model.TableName())
	if ts == nil {
		return fmt.Errorf("table[%s] spec not found", model.TableName())
	}
	if id == 0 {
		return fmt.Errorf("id is zero")
	}

	return dao.ds.reader.Get(emptyTableModel, ts.getSelectSql(1), id)
}

func (dao *SqliteDao) TableSelect(emptyTableSlice interface{}, ids ...int64) error {
	if len(ids) == 0 {
		return fmt.Errorf("ids is empty")
	}

	// 确认 emptyTableSlice 是一个切片的指针
	sliceValue := reflect.ValueOf(emptyTableSlice)
	if sliceValue.Kind() != reflect.Ptr || sliceValue.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("emptyTableSlice must be a pointer to a slice")
	}

	// 获取切片的元素类型
	elemType := sliceValue.Elem().Type().Elem()

	// 使用反射创建一个实例
	newElem := reflect.New(elemType).Elem()

	// 将其转换为 ITable 接口类型
	emptyTableModel, ok := newElem.Interface().(ITable)
	if !ok {
		return fmt.Errorf("failed to convert new element to ITable")
	}

	ts := dao.ds.GetTableSpec(emptyTableModel.TableName())
	if ts == nil {
		return fmt.Errorf("table[%s] spec not found", emptyTableModel.TableName())
	}

	return dao.ds.reader.Select(emptyTableSlice, ts.getSelectSql(len(ids)), ids)
}

func (dao *SqliteDao) Conn() *sqlx.DB {
	return dao.ds.reader
}
