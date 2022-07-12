package util

import "os"

// PathExist check whether the directory or file is exists
func PathExist(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}
