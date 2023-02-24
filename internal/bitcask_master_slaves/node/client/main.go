package main

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/client/client"
	"flag"
)

func main() {
	var host, port string
	flag.StringVar(&host, "host", "localhost", "server host")
	flag.StringVar(&port, "port", "8888", "server port")
	flag.Parse()
	client.NewClient("tcp", host, port).Start()
}
