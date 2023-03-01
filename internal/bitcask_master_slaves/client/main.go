package main

import (
	"bitcaskDB/internal/bitcask_master_slaves/client/client"
	"flag"
)

func main() {
	var host, port, serverType string
	flag.StringVar(&host, "host", "localhost", "server host")
	flag.StringVar(&port, "port", "8888", "server port")
	flag.StringVar(&serverType, "type", "node", "node or proxy")
	flag.Parse()
	client.NewClient("tcp", host, port).Start()
}
