package benchmark

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/client"
	"bitcaskDB/internal/options"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var db *bitcask.BitcaskDB

func init() {
	// path := filepath.Join("/data")
	path := "E:" + string(os.PathSeparator) + "tmp" + string(os.PathSeparator) + "test"
	opts := options.DefaultOptions(path)
	var err error
	db, err = bitcask.Open(opts)
	if err != nil {
		fmt.Printf("open bitcaskDB err: %v", err)
		return
	}

	initDataForGet()
}

func initDataForGet() {
	writeCount := 1800000
	for i := 0; i < writeCount; i++ {
		err := db.Set(getKey(i), getValue128B())
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkDB_Set(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Set(getKey(i), getValue128B())
		assert.Nil(b, err)
	}
}

func BenchmarkDB_Get(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(getKey(i))
		assert.Nil(b, err)
	}
}

func BenchmarkDB_LPush(b *testing.B) {
	keys := [][]byte{
		[]byte("my_list-1"),
		[]byte("my_list-2"),
		[]byte("my_list-3"),
		[]byte("my_list-4"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := rand.Int() % len(keys)
		err := db.LPush(keys[k], getValue128B())
		assert.Nil(b, err)
	}
}

func BenchmarkDB_ZAdd(b *testing.B) {
	keys := [][]byte{
		[]byte("my_zset-1"),
		[]byte("my_zset-2"),
		[]byte("my_zset-3"),
		[]byte("my_zset-4"),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := rand.Int() % len(keys)
		err := db.ZAdd(keys[k], float64(i+100), getValue128B())
		assert.Nil(b, err)
	}
}

func Benchmark_Parallel_Cli_Set(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		cl := client.NewClient("tcp", "127.0.0.1", "55201")
		for pb.Next() {
			cl.Send([]byte(fmt.Sprintf("set %s %s", getKey(123123), getValue128B())))
			_, err := cl.Receive()
			assert.Nil(b, err)

		}
	})
}

func Benchmark_Cli_Set(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	cl := client.NewClient("tcp", "127.0.0.1", "55201")
	for i := 0; i < b.N; i++ {
		cl.Send([]byte(fmt.Sprintf("set %s %s", getKey(123123), getValue128B())))
		_, err := cl.Receive()
		assert.Nil(b, err)
	}
}

func Benchmark_Parallel_Cli_Get(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		cl := client.NewClient("tcp", "127.0.0.1", "55201")
		for pb.Next() {
			cl.Send([]byte(fmt.Sprintf("get %s", getKey(rand.Intn(100000)))))
			_, err := cl.Receive()
			assert.Nil(b, err)

		}
	})
}
func BenchmarkCli_Get(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	cl := client.NewClient("tcp", "127.0.0.1", "55201")
	for i := 0; i < b.N; i++ {
		cl.Send([]byte(fmt.Sprintf("get %s", getKey(rand.Intn(100000)))))
		_, err := cl.Receive()
		assert.Nil(b, err)
	}
}
