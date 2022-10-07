package main

import (
	"bitcaskDB/internal/log"
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

var (
	defaultHost = "127.0.0.1"
	defaultPort = "55201"
)

const (
	MaxHeaderLength = 12 // Use up to 12 bytes to represent the length
	CmdBufferSize   = 10
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
	log.Info("Client address : ", conn.LocalAddr().String())
	// conn.LocalAddr().String()
	if err != nil {
		log.Errorf("dial err : %v", err)
	}
	defer conn.Close()

	input := bufio.NewReader(os.Stdin)
	for {
		// Read CMD
		fmt.Printf("%s> ", serverOpts.host+":"+serverOpts.port)
		cmdStr, _ := input.ReadString('\n')
		cmdStr = strings.Trim(cmdStr, "\r\n")

		// Handle CMD
		// The format of msg is [header(the length of data) + data]
		cmdByteArr := []byte(cmdStr)
		header := make([]byte, MaxHeaderLength)
		n := binary.PutUvarint(header, uint64(len(cmdByteArr)))
		msg := make([]byte, n+len(cmdByteArr))
		copy(msg, header[:n])
		copy(msg[n:], cmdByteArr)

		// Send CMD
		if _, err := conn.Write([]byte(msg)); err != nil {
			log.Errorf("conn write err %v", err)
			return
		}

		// Receive result
		buffer := make([]byte, CmdBufferSize)
		n, err := conn.Read(buffer) // block read....
		if err != nil {
			log.Errorf("conn read err : %v", err)
			return
		}

		length, offset := binary.Uvarint(buffer)
		if int(length) <= CmdBufferSize-offset { // buffer is large enough to receive the msg
			buffer = buffer[offset:n]
		} else {
			tmp := buffer[offset:]
			buffer = make([]byte, int(length)) // make a new buffer, which is large enough to receive the msg
			copy(buffer, tmp)
			_, err := conn.Read(buffer[n-offset:]) // 这里是否该改成非阻塞读？
			if err != nil {
				log.Errorf("conn read err : %v", err)
				return
			}
		}

		// var buffer [1024]byte
		// n, err := conn.Read(buffer[:])
		// if err != nil {
		// 	if err != io.EOF {
		// 		log.Errorf("conn read err %v", err)
		// 	}
		// 	return
		// }
		// Show result
		fmt.Println(string(buffer))
	}
}
