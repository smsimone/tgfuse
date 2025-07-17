//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package atime

import (
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func getAtime(fi os.FileInfo) *time.Time {
	stat, ok := fi.Sys().(*unix.Stat_t)
	if !ok {
		return nil
	}

	var sec, nsec int64

	// macOS e BSD usano Atimespec
	// Linux usa Atim
	switch {
	case stat.Atim.Sec != 0 || stat.Atim.Nsec != 0:
		sec = int64(stat.Atim.Sec)
		nsec = int64(stat.Atim.Nsec)
	// case stat.Atimespec.Sec != 0 || stat.Atimespec.Nsec != 0:
	// 	sec = int64(stat.Atimespec.Sec)
	// 	nsec = int64(stat.Atimespec.Nsec)
	default:
		return nil
	}

	at := time.Unix(sec, nsec)
	return &at
}
