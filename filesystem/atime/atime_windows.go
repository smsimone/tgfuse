//go:build windows

package atime

import (
	"os"
	"syscall"
	"time"
)

func getAtime(fi os.FileInfo) *time.Time {
	stat := fi.Sys().(*syscall.Win32FileAttributeData)
	// FILETIME is in 100-nanosecond intervals since January 1, 1601 (UTC)
	ft := stat.LastAccessTime
	nsec := ft.Nanoseconds()
	at := time.Unix(0, nsec)
	return &at
}
