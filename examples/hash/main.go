package main

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/options"
	"fmt"
	"os"
)

func main() {
	path := "D:" + string(os.PathSeparator) + "test"
	opts := options.DefaultOptions(path)
	db, err := bitcask.Open(opts)
	if err != nil {
		fmt.Printf("open bitcaskDB err: %v", err)
		return
	}

	err = db.HSet([]byte("watermelon"), []byte("hash"), []byte("In summer, I love watermelon."))
	if err != nil {
		fmt.Printf("HSet error: %v", err)
	}

	value, err := db.HGet([]byte("watermelon"), []byte("hash"))
	if err != nil {
		fmt.Printf("HGet error: %v", err)
	}
	fmt.Println(string(value))

	exist, err := db.HExists([]byte("watermelon"), []byte("hash"))
	if err != nil {
		fmt.Printf("HExists error: %v", err)
	}
	if exist {
		fmt.Println("Hash key watermelon exist.")
	}

	fields, err := db.HFields([]byte("watermelon"))
	if err != nil {
		fmt.Printf("Hkeys error: %v", err)
	}
	fmt.Println("The fields in watermelon are:", fields)

	ok, err := db.HSetNX([]byte("key-1"), []byte("field-1"), []byte("value-1"))
	if err != nil {
		fmt.Printf("HSetNx error: %v", err)
	}
	fmt.Println(ok)

	value, err = db.HGet([]byte("key-1"), []byte("field-1"))
	if err != nil {
		fmt.Printf("Error when key-1/field-1 is trying to get: %v", err)
	}
	fmt.Printf("key-1/value-1: %s", string(value))

	_ = db.HSet([]byte("my_hash"), []byte("f1"), []byte("val-1"), []byte("f2"), []byte("val-2"))
	values, err := db.HMGet([]byte("my_hash"), []byte("f1"), []byte("f2"))
	if err != nil {
		fmt.Printf("hmget err: %v", err)
		return
	}
	fmt.Println("\n-----hmget results-----")
	for _, v := range values {
		fmt.Println(string(v))
	}

	res, err := db.HDel([]byte("my_hash"), []byte("f1"))
	if err != nil {
		fmt.Printf("hdel err: %v", err)
		return
	}
	fmt.Println("hdel result : ", res)
	db.Close()
}
