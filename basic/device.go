package basic

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

// 获取设备唯一 ID 的跨平台实现
func GetDeviceID() (string, error) {
	switch runtime.GOOS {
	case "windows":
		return getWindowsDeviceID()
	case "darwin":
		return getMacDeviceID()
	case "linux":
		return getLinuxDeviceID()
	default:
		return "", fmt.Errorf("unsupported platform: %s", runtime.GOOS)
	}
}

// 在 Windows 上，使用 wmic 命令可以获取计算机主板的 UUID
func getWindowsDeviceID() (string, error) {
	cmd := exec.Command("wmic", "csproduct", "get", "UUID")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return "", err
	}
	lines := strings.Split(out.String(), "\n")
	if len(lines) < 2 {
		return "", fmt.Errorf("unexpected output from wmic")
	}
	return strings.TrimSpace(lines[1]), nil
}

// 在 Linux 上，可以读取 /etc/machine-id 文件，它通常存储系统的唯一标识。
func getLinuxDeviceID() (string, error) {
	data, err := os.ReadFile("/etc/machine-id")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// 在 macOS 上，可以使用 ioreg 命令来获取硬件 UUID。
func getMacDeviceID() (string, error) {
	cmd := exec.Command("ioreg", "-rd1", "-c", "IOPlatformExpertDevice")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return "", err
	}
	lines := strings.Split(out.String(), "\n")
	for _, line := range lines {
		if strings.Contains(line, "IOPlatformUUID") {
			parts := strings.Split(line, " = ")
			if len(parts) == 2 {
				return strings.Trim(parts[1], "\""), nil
			}
		}
	}
	return "", fmt.Errorf("IOPlatformUUID not found")
}
