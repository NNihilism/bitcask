package cmd

import (
	"bitcaskDB/internal/client"
	"flag"
)

var (
	defaultHost = "127.0.0.1"
	defaultPort = "55201"
)

type ServerOptions struct {
	host string
	port string
}

func main() {
	// init server options
	serverOpts := new(ServerOptions)
	flag.StringVar(&serverOpts.host, "host", defaultHost, "server host")
	flag.StringVar(&serverOpts.port, "port", defaultPort, "server port")
	flag.Parse()
	client.NewClient("tcp", serverOpts.host, serverOpts.port).Start()
}
