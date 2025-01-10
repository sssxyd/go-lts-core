package basic

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
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

func GetAvailableMemory() uint64 {
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

func GetCpuCount() int {
	return runtime.NumCPU()
}

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
