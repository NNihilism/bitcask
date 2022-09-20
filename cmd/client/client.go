package main

import (
	"bitcaskDB/internal/log"
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
)

var (
	defaultHost = "127.0.0.1"
	defaultPort = "5200"
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
	conn, err := net.Dial("tcp", serverOpts.host+":"+serverOpts.port)
	if err != nil {
		log.Errorf("dial err : %v", err)
	}
	defer conn.Close()

	input := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("%s> ", serverOpts.host+":"+serverOpts.port)
		s, _ := input.ReadString('\n')
		if _, err := conn.Write([]byte(s)); err != nil {
			log.Errorf("conn write err %v", err)
			return
		}

		var buffer [1024]byte
		n, err := conn.Read(buffer[:])
		if err != nil {
			log.Errorf("conn read err %v", err)
			return
		}
		fmt.Println(string(buffer[:n]))
	}

}
