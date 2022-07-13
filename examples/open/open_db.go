package main

import (
	"bitcask"
	"bitcask/options"
	"fmt"
	"os"
)

func main() {
	path := "D:" + string(os.PathSeparator) + "test"
	opts := options.DefaultOptions(path)
	_, err := bitcask.Open(opts)
	if err != nil {
		fmt.Printf("open rosedb err: %v", err)
		return
	}

}
