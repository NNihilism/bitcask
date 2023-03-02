package main

import (
	"bitcaskDB/internal/bitcask_master_slaves/client/nodeClient"
	"flag"
)

func main() {

	// Code for cmd...
	var host, port, serverType string
	flag.StringVar(&host, "host", "localhost", "server host")
	flag.StringVar(&port, "port", "8888", "server port")
	flag.StringVar(&serverType, "type", "node", "node or proxy")
	flag.Parse()
	nodeClient.NewClient("tcp", host, port).Start()

	// client := msClient.NewClient(&msClient.MSClientConfig{
	// 	MasterHost: "127.0.0.1",
	// 	MasterPort: "8991",
	// })
	// client.Set(util.StrArrToByteArr([]string{"12332", "123456789"}))
	// client.MSet(util.StrArrToByteArr([]string{"123", "321", "123322222", "1q2312312", "aas", "bbs"}))
}
