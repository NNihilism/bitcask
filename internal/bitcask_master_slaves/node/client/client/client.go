package client

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/consts"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	"bitcaskDB/internal/log"
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cloudwego/kitex/client"
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
	rpcClient  nodeservice.Client
	input      *bufio.Reader
	result     chan string
	Done       chan struct{}
	serverOpts ServerOptions
}

func NewClient(network, host, port string) *Client {
	// 初始化rpc客户端
	c, err := nodeservice.NewClient(
		consts.NodeServiceName,
		client.WithHostPorts(host+":"+port),
	)
	if err != nil {
		log.Errorf("Init rpcclient err [%v]", err)
		return nil
	}

	return &Client{
		rpcClient: c,
		input:     bufio.NewReader(os.Stdin),
		result:    make(chan string, 1),
		Done:      make(chan struct{}, 1),
		serverOpts: ServerOptions{
			host: host,
			port: port,
		},
	}
}

// 控制台只需要在标准输出中打印结果即可
func (cl *Client) WriteResult(result []byte) {
	fmt.Println(string(result))
}

// 解析用户输入，获取命令/参数
func (cl *Client) parse(cmd []byte) (cmdHandler, [][]byte, error) {
	parts := bytes.Split(bytes.TrimSpace(cmd), []byte(" "))
	command, args := bytes.ToLower(parts[0]), parts[1:]
	cmdFunc, ok := supportedCommands[string(command)]
	fmt.Println("command:", string(command))
	fmt.Println("ok:", ok)
	if !ok {
		// cl.WriteResult([]byte(util.NewErrUnknownCMD(command, args).Error()))
		return nil, nil, errno.NewErr(errno.ErrUnknownCMD, &errno.ErrInfo{Cmd: string(command)})
	}

	return cmdFunc, args, nil
	/*
		if string(command) == "quit" {
			return nil, nil, nil
		}

		if res, err := cmdFunc(cl, args); err != nil {
			if err == bitcask.ErrKeyNotFound {
				cl.WriteResult([]byte("(nil)"))
			} else {
				cl.WriteResult([]byte("(error) " + err.Error()))
			}
		} else {
			// 通过反射判断数据类型，再统一转成[]byte形式？
			// cli.conn.Write(util.ConvertToBSlice(res))
			cl.WriteResult(util.ConvertToBSlice(res))
		}
	*/
}

func (cl *Client) Start() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		for {
			cmd := cl.read()

			cmdFunc, args, err := cl.parse(cmd)

			if err != nil {
				cl.WriteResult([]byte("(error) " + err.Error()))
				continue
			}

			result, err := cmdFunc(cl, args)
			if err != nil {
				cl.WriteResult([]byte("(error) " + err.Error()))
				continue
			}
			cl.WriteResult(result.([]byte))
			// if err := cl.send(cmd); err != nil {
			// 	cl.done()
			// 	return
			// }

			// go cl.receive()

			// select {
			// case result := <-cl.result:
			// 	fmt.Println(result)
			// case <-time.After(DefaultTimeOut):
			// 	log.Error("Receive Timeout")
			// 	return
			// }
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
	// Loop until get valid input
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

// // Send sent cmd to server
// func (cl *Client) send(cmd []byte) error {
// 	if _, err := cl.conn.Write(cmd); err != nil {
// 		log.Errorf("conn write err %v", err)
// 		return err
// 	}
// 	return nil
// }

// // Receive result from server
// func (cl *Client) receive() {
// 	// Receive result
// 	buffer := make([]byte, CmdBufferSize)
// 	n, err := cl.conn.Read(buffer) // block read....
// 	if err != nil {
// 		if err != io.EOF {
// 			log.Errorf("conn read err : %v", err)
// 		}
// 		cl.done()
// 		return
// 	}

// 	length, offset := binary.Uvarint(buffer)
// 	if int(length) <= CmdBufferSize-offset { // buffer is large enough to receive the msg
// 		buffer = buffer[offset:n]
// 	} else {
// 		tmp := buffer[offset:]
// 		buffer = make([]byte, int(length)) // make a new buffer, which is large enough to receive the msg
// 		copy(buffer, tmp)
// 		_, err := cl.conn.Read(buffer[n-offset:]) // Should change to non-block read?
// 		if err != nil {
// 			log.Errorf("conn read err : %v", err)
// 			cl.done()

// 		}
// 	}

// 	cl.result <- string(buffer)
// }

func (cl *Client) Close() {
	// if err := cl.rpcClient.; err != nil {
	// 	log.Errorf("close err : [%v]", err)
	// }
}

func (cl *Client) done() {
	cl.Done <- struct{}{}
}

// Send sent cmd to server
// Just for benchmark test.
// func (cl *Client) Send(cmd []byte) error {
// if _, err := cl.conn.Write(cmd); err != nil {
// 	log.Errorf("conn write err %v", err)
// 	return err
// }
// return nil
// }

// Receive result from server
// Just for benchmark test.
// func (cl *Client) Receive() (string, error) {
// 	// Receive result
// 	buffer := make([]byte, CmdBufferSize)
// 	n, err := cl.conn.Read(buffer) // block read....
// 	if err != nil {
// 		if err != io.EOF {
// 			log.Errorf("conn read err : %v", err)
// 		}
// 		// cl.done()
// 		return "", err
// 	}

// 	length, offset := binary.Uvarint(buffer)
// 	if int(length) <= CmdBufferSize-offset { // buffer is large enough to receive the msg
// 		buffer = buffer[offset:n]
// 	} else {
// 		tmp := buffer[offset:]
// 		buffer = make([]byte, int(length)) // make a new buffer, which is large enough to receive the msg
// 		copy(buffer, tmp)
// 		_, err := cl.conn.Read(buffer[n-offset:]) // Should change to non-block read?
// 		if err != nil {
// 			log.Errorf("conn read err : %v", err)
// 			// cl.done()
// 			return "", err
// 		}
// 	}
// 	return string(buffer), nil
// 	// cl.result <- string(buffer)
// }
