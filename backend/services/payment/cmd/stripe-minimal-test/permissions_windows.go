//go:build stripe_minimal_test && windows

package main

import "os"

// Windows does not expose POSIX permission bits in os.FileMode. This harness
// runs on Linux; Windows builds only support local compile and state-machine
// tests, so regular-file validation is the strongest portable check here.
func privateRegularFile(info os.FileInfo) bool {
	return info != nil && info.Mode().IsRegular()
}

func syncParentDirectory(string) error { return nil }
