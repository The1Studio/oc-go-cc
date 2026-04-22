//go:build !windows

package telemetry

import (
	"os"
	"syscall"
)

// flock acquires an exclusive advisory lock on f.
func flock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
}

// funlock releases the advisory lock on f.
func funlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
