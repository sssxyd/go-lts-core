package rdbms

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sssxyd/go-lts-core/basic"
	_ "modernc.org/sqlite"
)

type SqliteDataSource struct {
	id         string
	db_path    string
	writer     *sqlx.DB
	reader     *sqlx.DB
	tableSpecs sync.Map
	doTasks    chan SqlTask
	wg         sync.WaitGroup
}

func newSqliteDataSource(id string, db_path string, statements []string) (*SqliteDataSource, error) {
	// 检查文件是否存在
	// 创建目录
	err := os.MkdirAll(filepath.Dir(db_path), os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// 检查数据库文件是否存在，若不存在则创建文件
	db_file_exists := false
	if _, err := os.Stat(db_path); err == nil {
		db_file_exists = true
		log.Printf("database file[%s] already exists\n", db_path)
	} else {
		file, err := os.OpenFile(db_path, os.O_CREATE|os.O_EXCL, os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("failed to create database file: %w", err)
		}
		file.Close()
	}

	writer, err := create_writer(db_path)
	if err != nil {
		log.Printf("failed to create writer: %v\n", err)
		return nil, err
	}
	if !db_file_exists && len(statements) > 0 {
		for _, statement := range statements {
			if statement == "" {
				continue
			}
			log.Printf("executing statement: %s\n", statement)
			_, err := writer.Exec(statement)
			if err != nil {
				log.Printf("failed to execute statement: %v\n", err)
				writer.Close()
				return nil, err
			}
		}
	}

	reader, err := create_reader(db_path, 64*1024*1024)
	if err != nil {
		log.Printf("failed to create reader: %v\n", err)
		writer.Close()
		return nil, err
	}

	ds := &SqliteDataSource{
		id:         id,
		db_path:    db_path,
		writer:     writer,
		reader:     reader,
		tableSpecs: sync.Map{},
		doTasks:    make(chan SqlTask, 1000),
		wg:         sync.WaitGroup{},
	}

	// 启动后台写任务
	ds.wg.Add(1)
	go do_sql_task_background(writer, ds.doTasks, &ds.wg)

	return ds, nil
}

func create_writer(db_path string) (*sqlx.DB, error) {
	writer, err := sqlx.Connect("sqlite", db_path)
	if err != nil {
		log.Printf("failed to connect to database: %v\n", err)
		return nil, err
	}

	// 设置数据库 WAL 模式
	if _, err := writer.Exec("PRAGMA journal_mode = WAL"); err != nil {
		writer.Close()
		log.Printf("failed to set journal mode: %v\n", err)
		return nil, err
	}

	// 关闭连接池
	writer.SetMaxOpenConns(1)    // 只允许一个连接
	writer.SetMaxIdleConns(1)    // 避免创建额外的空闲连接
	writer.SetConnMaxLifetime(0) // 禁止连接超时
	return writer, nil
}

func create_reader(path string, max_memory_map_size uint64) (*sqlx.DB, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("database file does not exist: %w", err)
	}

	// 获取可用内存的一半，默认64MB
	freeMemory := basic.GetAvailableMemory()
	if freeMemory <= 0 {
		freeMemory = 64 * 1024 * 1024
	} else {
		freeMemory /= 2
	}

	fileSize := uint64(fileInfo.Size())
	mmapSize := calculateMmapSize(fileSize, freeMemory, max_memory_map_size)

	// 设置缓存大小：全部映射时象征性设置为1MB，否则为映射大小的1/10
	cacheSize := calculateCacheSize(mmapSize, fileSize)

	// 获取CPU数量并确保最小为1
	cpuCount := basic.GetCpuCount()
	if cpuCount < 1 {
		cpuCount = 1
	}

	// 连接数据库
	db, err := sqlx.Connect("sqlite", fmt.Sprintf("file:%s?cache=shared&mode=ro", path))
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	// 设置数据库参数并处理错误
	if _, err := db.Exec("PRAGMA cache_size = -" + fmt.Sprint(cacheSize/1024)); err != nil {
		return nil, fmt.Errorf("failed to set cache size: %w", err)
	}
	if _, err := db.Exec("PRAGMA mmap_size = " + fmt.Sprint(mmapSize)); err != nil {
		return nil, fmt.Errorf("failed to set mmap size: %w", err)
	}
	if _, err := db.Exec("PRAGMA temp_store = MEMORY"); err != nil {
		return nil, fmt.Errorf("failed to set temp_store: %w", err)
	}
	if _, err := db.Exec("PRAGMA synchronous = OFF"); err != nil {
		return nil, fmt.Errorf("failed to set synchronous mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA query_only = true"); err != nil {
		return nil, fmt.Errorf("failed to set journal mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys = OFF"); err != nil {
		return nil, fmt.Errorf("failed to disable foreign keys: %w", err)
	}

	// 设置连接池
	db.SetMaxOpenConns(cpuCount * 10)
	db.SetMaxIdleConns(cpuCount)
	db.SetConnMaxLifetime(6 * time.Hour)
	db.SetConnMaxIdleTime(10 * time.Minute)

	return db, nil
}

// calculateMmapSize 计算 mmap 大小
func calculateMmapSize(fileSize, freeMemory, maxMemoryMapSize uint64) uint64 {
	switch {
	case fileSize < 64*1024*1024:
		return fileSize
	case maxMemoryMapSize > 0:
		return min(fileSize, maxMemoryMapSize, freeMemory)
	default:
		return min(fileSize, freeMemory)
	}
}

// calculateCacheSize 计算缓存大小
func calculateCacheSize(mmapSize, fileSize uint64) uint64 {
	baseCacheSize := uint64(1024 * 1024) // 1 MB
	if mmapSize < fileSize {
		return max(uint64(mmapSize/10), baseCacheSize)
	}
	return baseCacheSize
}

func do_sql_task_background(writer *sqlx.DB, taskChannel <-chan SqlTask, wg *sync.WaitGroup) {
	defer wg.Done()

	for task := range taskChannel {
		result := SqlResult{
			LastInsertID: make([]int64, 0),
			RowsAffected: 0,
			Err:          nil,
		}
		if task.SQL == "" {
			task.Result <- result
			continue
		}

		var ret sql.Result
		var err error
		if task.BatchArgs != nil && len(task.BatchArgs) > 0 {
			tx, err := writer.Beginx()
			if err != nil {
				result.Err = err
				task.Result <- result
				continue
			}

			for _, args := range task.BatchArgs {
				if args == nil || len(args) == 0 {
					ret, err = tx.Exec(task.SQL)
				} else {
					ret, err = tx.Exec(task.SQL, args...)
				}
				if err != nil {
					tx.Rollback() // 执行回滚
					result.Err = err
					log.Printf("Error during batch execution: %v\n", err) // 增加日志记录
					break
				}
			}

			// 批量操作成功，提交事务
			if result.Err == nil {
				err = tx.Commit()
				if err != nil {
					result.Err = err
					log.Printf("Failed to commit transaction: %v\n", err)
				}
			}
		} else {
			if task.Args != nil && len(task.Args) > 0 {
				ret, err = writer.Exec(task.SQL, task.Args...)
			} else {
				ret, err = writer.Exec(task.SQL)
			}
			if err != nil {
				result.Err = err
				log.Printf("Error executing SQL: %v\n", err) // 增加日志记录
			}
		}

		// 记录执行结果
		if ret != nil {
			lastInsertID, _ := ret.LastInsertId()
			if lastInsertID < 0 {
				lastInsertID = 0
			}
			result.LastInsertID = append(result.LastInsertID, lastInsertID)
			rowsAffected, _ := ret.RowsAffected()
			result.RowsAffected += rowsAffected
		}

		task.Result <- result
	}
}

func (ds *SqliteDataSource) Id() string {
	return ds.id
}

func (ds *SqliteDataSource) Type() string {
	return "sqlite"
}

func (ds *SqliteDataSource) Host() string {
	return ""
}

func (ds *SqliteDataSource) Port() int {
	return 0
}

func (ds *SqliteDataSource) Database() string {
	return ds.db_path
}

func (ds *SqliteDataSource) Username() string {
	return ""
}

func (ds *SqliteDataSource) Password() string {
	return ""
}

func (ds *SqliteDataSource) ScanTable(models ...ITable) {
	scan_table(&ds.tableSpecs, models...)
}

func (ds *SqliteDataSource) GetTableSpec(tableName string) *TableSpec {
	if ts, ok := ds.tableSpecs.Load(tableName); ok {
		return ts.(*TableSpec)
	}
	return nil
}

func (ds *SqliteDataSource) NewDao() IDao {
	return &SqliteDao{ds: ds}
}

func (ds *SqliteDataSource) Close() error {
	log.Printf("closing sqlite data source[%s]\n", ds.id)
	if ds == nil {
		log.Printf("sqlite data source is nil\n")
		return nil
	}
	if ds.doTasks == nil {
		log.Printf("sqlite data source doTasks is nil\n")
		return nil
	}
	close(ds.doTasks)
	// 等待后台任务结束
	ds.wg.Wait()
	log.Printf("sqlite data source[%s] tasks done\n", ds.id)

	// 关闭读数据库连接
	err := ds.reader.Close()
	if err != nil {
		return err
	}
	log.Printf("%s's reader closed\n", ds.id)
	// 关闭写数据库连接
	err = ds.writer.Close()
	if err != nil {
		return err
	}
	log.Printf("%s's writer closed\n", ds.id)
	return nil
}
