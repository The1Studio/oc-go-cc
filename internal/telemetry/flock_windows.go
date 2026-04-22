//go:build windows

package telemetry

import "os"

// flock is a no-op on Windows.
func flock(f *os.File) error { return nil }

// funlock is a no-op on Windows.
func funlock(f *os.File) error { return nil }
