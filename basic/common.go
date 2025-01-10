package basic

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func StrToMD5(text string) string {
	hash := md5.Sum([]byte(text))
	md5String := hex.EncodeToString(hash[:])
	return md5String
}

func BytesToBase64(bytes []byte) string {
	return base64.StdEncoding.EncodeToString(bytes)
}

func Base64ToBytes(base64Str string) []byte {
	bytes, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		fmt.Println("Error:", err)
		return nil
	}
	return bytes
}

func BytesToHex(bytes []byte) string {
	return hex.EncodeToString(bytes)
}

func HexToBytes(hexStr string) []byte {
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		fmt.Println("Error:", err)
		return nil
	}
	return bytes
}

func IsPathExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

func TouchDir(path string) error {
	// 获取路径的信息
	_, err := os.Stat(path)

	if err == nil { // 如果路径已存在
		return nil
	}

	if os.IsNotExist(err) { // 路径不存在
		// 检查路径是否包含扩展名，判断是文件还是目录
		if filepath.Ext(path) != "" {
			// 这是一个文件路径，创建其父目录
			dir := filepath.Dir(path)
			fmt.Printf("Creating directory for file: %s\n", dir)
			return os.MkdirAll(dir, 0755)
		} else {
			// 这是一个目录路径，创建目录
			fmt.Printf("Creating directory: %s\n", path)
			return os.MkdirAll(path, 0755)
		}
	}

	// 如果出现其他错误
	return err
}

func GetFileInfo(file_path string) (os.FileInfo, error) {
	fileInfo, err := os.Stat(file_path)
	if err != nil {
		return nil, err
	}
	return fileInfo, nil
}

func JsonToStruct[T any](json_data string) (T, error) {
	var result T
	err := json.Unmarshal([]byte(json_data), &result)
	if err != nil {
		return result, err
	}
	return result, nil
}

func StructToJson[T any](data T) (string, error) {
	json_data, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(json_data), nil
}
