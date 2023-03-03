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
		fmt.Printf("open bitcask err: %v", err)
		return
	}

	// Strlen
	key := []byte("Key - Test")
	key1 := []byte("KKKKKKKey - Test")

	sLen1 := db.StrLen(key1)
	sLen := db.StrLen(key)
	fmt.Println("sLen1:", sLen1)
	fmt.Println("sLen:", sLen)

	// Count
	fmt.Println("count:", db.Count())
	// time.Sleep(time.Second * 3)

	// // Set
	// key := []byte("Key - Test")
	// value := []byte("Yeah ! You get the value of the key")
	// db.Set(key, value)
	// val, err := db.Get(key)
	// if err != nil {
	// 	fmt.Println("Failed to get value")
	// }
	// fmt.Println("val : ", string(val))

	// // Delete
	// err = db.Delete(key)
	// if err != nil {
	// 	fmt.Println("Failed to delete")
	// }
	// val, err = db.Get(key)
	// if err != nil {
	// 	fmt.Println("Delete key successfully.")
	// }
	// fmt.Println("value : ", string(val))

	// // SetEx
	// db.SetEX(key, []byte("Yeah ! You get the value of the key"), time.Second*2)
	// fmt.Println("Set key with 2s")
	// val, err = db.Get(key)
	// if err != nil {
	// 	fmt.Println("Failed to get value")
	// } else {
	// 	fmt.Println("succeed in getting value before 2s:", string(val))
	// }
	// time.Sleep(time.Second * 3)
	// fmt.Println("wake up")
	// _, err = db.Get(key)
	// if err != nil {
	// 	fmt.Println("err : ", err)
	// }

	// // MSet
	// key1 := []byte("KKKKKKKey - Test")
	// value1 := []byte("YYYYYYYeah ! You get the value of the key")
	// db.MSet(key, val, key, val, key1, value1)
	// val1, _ := db.Get(key1)
	// fmt.Println(string(val1))

	db.Close()

}
