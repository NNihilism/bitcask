package benchmark

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"
)

const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

var key2 [][]byte

func init() {
	rand.Seed(time.Now().Unix())
	writeCount := 180000
	for i := 0; i < writeCount; i++ {
		key2 = append(key2, getKey(i))
	}
}

// GetKey length: 32 Bytes
func getKey(n int) []byte {
	return []byte("kvstore-bench-key------" + fmt.Sprintf("%09d", n))
}

func getKey2(n int) [][]byte {
	return [][]byte{[]byte("kvstore-bench-key------" + fmt.Sprintf("%09d", n))}
}

func getKeyAndValue(n int) [][]byte {
	return [][]byte{getKey(n), getValue128B()}
}

// GetValue128B .
func getValue128B() []byte {
	var str bytes.Buffer
	for i := 0; i < 128; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	// return []byte(str.String())
	return str.Bytes()
}

// GetValue4K .
func getValue4K() []byte {
	var str bytes.Buffer
	for i := 0; i < 4096; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return str.Bytes()
	// return []byte(str.String())
}
