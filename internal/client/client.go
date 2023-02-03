package client

import (
	"bitcaskDB/internal/log"
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	MaxHeaderLength = 12 // Use up to 12 bytes to represent the length
	CmdBufferSize   = 10
	DefaultTimeOut  = 5 * time.Second
)

type ServerOptions struct {
	host string
	port string
}

type Client struct {
	conn       net.Conn
	input      *bufio.Reader
	result     chan string
	Done       chan struct{}
	serverOpts ServerOptions
}

func NewClient(network, host, port string) *Client {
	conn, err := net.Dial(network, host+":"+port)
	// conn.SetDeadline(time.Now().Add(time.Second))	// cancel block
	// time.After()
	// t := time.AfterFunc(time.Second, func() {})
	// t.Reset()
	if err != nil {
		log.Errorf("Dial err [%v]", err)
		return nil
	}

	return &Client{
		conn:   conn,
		input:  bufio.NewReader(os.Stdin),
		result: make(chan string, 1),
		Done:   make(chan struct{}, 1),
		serverOpts: ServerOptions{
			host: host,
			port: port,
		},
	}
}

func (cl *Client) Start() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		for {
			cmd := cl.read()

			if err := cl.send(cmd); err != nil {
				cl.done()
				return
			}

			go cl.receive()

			select {
			case result := <-cl.result:
				fmt.Println(result)
			case <-time.After(DefaultTimeOut):
				log.Error("Receive Timeout")
				return
			}
		}
	}()

	select {
	case <-sig:
		cl.Close()
	case <-cl.Done:
		cl.Close()
	}
}

// Read get user input from the console
func (cl *Client) read() []byte {
	// Read CMD
	for {
		fmt.Printf("%s> ", cl.serverOpts.host+":"+cl.serverOpts.port)
		cmdStr, _ := cl.input.ReadString('\n')
		cmdStr = strings.Trim(cmdStr, "\r\n")

		if len(cmdStr) == 0 {
			continue
		}

		// Handle CMD
		// The format of msg is [header(the length of data) + data]
		cmdByteArr := []byte(cmdStr)
		header := make([]byte, MaxHeaderLength)
		n := binary.PutUvarint(header, uint64(len(cmdByteArr)))
		cmd := make([]byte, n+len(cmdByteArr))
		copy(cmd, header[:n])
		copy(cmd[n:], cmdByteArr)

		return cmd
	}
}

// Send sent cmd to server
func (cl *Client) send(cmd []byte) error {
	if _, err := cl.conn.Write(cmd); err != nil {
		log.Errorf("conn write err %v", err)
		return err
	}
	return nil
}

// Receive result from server
func (cl *Client) receive() {
	// Receive result
	buffer := make([]byte, CmdBufferSize)
	n, err := cl.conn.Read(buffer) // block read....
	if err != nil {
		if err != io.EOF {
			log.Errorf("conn read err : %v", err)
		}
		cl.done()
		return
	}

	length, offset := binary.Uvarint(buffer)
	if int(length) <= CmdBufferSize-offset { // buffer is large enough to receive the msg
		buffer = buffer[offset:n]
	} else {
		tmp := buffer[offset:]
		buffer = make([]byte, int(length)) // make a new buffer, which is large enough to receive the msg
		copy(buffer, tmp)
		_, err := cl.conn.Read(buffer[n-offset:]) // Should change to non-block read?
		if err != nil {
			log.Errorf("conn read err : %v", err)
			cl.done()

		}
	}

	cl.result <- string(buffer)
}

func (cl *Client) Close() {
	if err := cl.conn.Close(); err != nil {
		log.Errorf("close err : [%v]", err)
	}
}

func (cl *Client) done() {
	cl.Done <- struct{}{}
}

// Send sent cmd to server
// Just for benchmark test.
func (cl *Client) Send(cmd []byte) error {
	if _, err := cl.conn.Write(cmd); err != nil {
		log.Errorf("conn write err %v", err)
		return err
	}
	return nil
}

// Receive result from server
// Just for benchmark test.
func (cl *Client) Receive() (string, error) {
	// Receive result
	buffer := make([]byte, CmdBufferSize)
	n, err := cl.conn.Read(buffer) // block read....
	if err != nil {
		if err != io.EOF {
			log.Errorf("conn read err : %v", err)
		}
		// cl.done()
		return "", err
	}

	length, offset := binary.Uvarint(buffer)
	if int(length) <= CmdBufferSize-offset { // buffer is large enough to receive the msg
		buffer = buffer[offset:n]
	} else {
		tmp := buffer[offset:]
		buffer = make([]byte, int(length)) // make a new buffer, which is large enough to receive the msg
		copy(buffer, tmp)
		_, err := cl.conn.Read(buffer[n-offset:]) // Should change to non-block read?
		if err != nil {
			log.Errorf("conn read err : %v", err)
			// cl.done()
			return "", err
		}
	}
	return string(buffer), nil
	// cl.result <- string(buffer)
}
