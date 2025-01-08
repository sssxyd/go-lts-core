package rdbms

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unsafe"
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
			fieldNameDBTags:   fileNameDBTags,
			dbTagFieldNames:   dbTagFieldNames,
			dbTagFieldIndexes: dbTagFieldIndexes,
		}
		tableSpecs.Store(tableName, ts)
	}
}

func get_available_memory() uint64 {
	switch runtime.GOOS {
	case "linux", "darwin":
		file, err := os.Open("/proc/meminfo")
		if err != nil {
			return 0
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "MemAvailable:") {
				fields := strings.Fields(line)
				availableMemoryKB, err := strconv.ParseUint(fields[1], 10, 64)
				if err != nil {
					return 0
				}
				// 返回值为 KB 转换为字节
				return availableMemoryKB * 1024
			}
		}
		return 0
	case "windows":
		kernel32 := syscall.NewLazyDLL("kernel32.dll")
		globalMemoryStatusEx := kernel32.NewProc("GlobalMemoryStatusEx")

		var memStatus memoryStatusEx
		memStatus.Length = uint32(unsafe.Sizeof(memStatus))

		ret, _, _ := globalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&memStatus)))
		if ret == 0 {
			return 0
		}
		return memStatus.AvailPhys
	default:
		return 0
	}
}

func get_cpu_count() int {
	return runtime.NumCPU()
}

func get_app_root_dir() string {
	exePath, err := os.Executable()
	if err != nil {
		fmt.Println("Error getting executable path:", err)
		return ""
	}
	exeDir := filepath.Dir(exePath)

	// 判断是否在临时目录中运行（典型的 go run 行为）
	if strings.Contains(exePath, os.TempDir()) {
		_, filename, _, ok := runtime.Caller(0)
		if !ok {
			fmt.Println("Failed to get caller information")
			return ""
		}
		return filepath.Dir(filepath.Dir(filepath.Dir(filename)))
	} else {
		// 默认返回可执行文件所在目录
		return exeDir
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

func SqlInValues(size int) string {
	placeholders := make([]string, size)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return "(" + strings.Join(placeholders, ",") + ")"
}
