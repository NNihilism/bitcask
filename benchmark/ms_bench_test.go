package benchmark

import (
	msClient "bitcaskDB/internal/bitcask_master_slaves/client/ms_client"
	"testing"

	"github.com/stretchr/testify/assert"
)

var cli *msClient.Client

func init() {
	cli = msClient.NewClient(&msClient.MSClientConfig{
		MasterHost: "127.0.0.1",
		MasterPort: "8991",
	})
	initDataForGetInMS()
}

func initDataForGetInMS() {
	writeCount := 18000
	for i := 0; i < writeCount; i++ {
		_, err := cli.Set(getKeyAndValue(i))
		if err != nil {
			panic(err)
		}
	}
}

/*
goos: linux
goarch: amd64
pkg: bitcaskDB/benchmark
cpu: AMD Ryzen 5 2500U with Radeon Vega Mobile Gfx
BenchmarkMS_Get-8   	    1861	    597979 ns/op	     526 B/op	      14 allocs/op
PASS
*/
func BenchmarkMS_Get(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := cli.Get([][]byte{key2[i]})
		assert.Nil(b, err)
	}
}
