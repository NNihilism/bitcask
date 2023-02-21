package main

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/options"
	"fmt"
	"os"
)

func main() {
	path := "E:" + string(os.PathSeparator) + "test"
	opts := options.DefaultOptions(path)
	db, err := bitcask.Open(opts)
	if err != nil {
		fmt.Printf("open rosedb err: %v", err)
		return
	}
	defer db.Close()

	_, err = db.SAdd([]byte("fruits"), []byte("watermelon"), []byte("grape"), []byte("orange"), []byte("apple"))
	if err != nil {
		fmt.Printf("SAdd error: %v", err)
	}

	_, err = db.SAdd([]byte("fav-fruits"), []byte("orange"), []byte("melon"), []byte("strawberry"))
	if err != nil {
		fmt.Printf("SAdd error: %v", err)
	}

	diffSet, err := db.SDiff([]byte("fruits"), []byte("fav-fruits"))
	if err != nil {
		fmt.Printf("SDiff error: %v", err)
	}
	fmt.Println("SDiff set:")
	for _, val := range diffSet {
		fmt.Printf("%v\n", string(val))
	}

	unionSet, err := db.SUnion([]byte("fruits"), []byte("fav-fruits"))
	if err != nil {
		fmt.Printf("SUnion error: %v", err)
	}
	fmt.Println("\nSUnion set:")
	for _, val := range unionSet {
		fmt.Printf("%v\n", string(val))
	}
}
