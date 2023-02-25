package client

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/consts"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	"bitcaskDB/internal/log"
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/signal"
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

	if !ok {
		return nil, nil, errno.NewErr(errno.ErrUnknownCMD, &errno.ErrInfo{Cmd: string(command)})
	}

	return cmdFunc, args, nil
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
		cmd, _ := cl.input.ReadBytes('\n')
		cmd = bytes.Trim(cmd, "\r\n")

		if len(cmd) == 0 {
			continue
		}
		return cmd
	}
}

func (cl *Client) Close() {

}

// func (cl *Client) done() {
// 	cl.Done <- struct{}{}
// }
