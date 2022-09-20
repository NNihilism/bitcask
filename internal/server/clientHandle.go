package server

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/log"
	"bytes"
	"fmt"
	"io"
)

const (
	cmdBufferSize = 1024
)

// var (
// ErrWrongNumberOfArgs = errors.New("wrong number of arguments")
// ErrWrongNumberOfArgs = errors.New("ERR unknown command `new`, with args beginning with:")
// )

type ClientHandle struct {
	conn io.ReadWriteCloser
	db   *bitcask.BitcaskDB
	// ctx  context.Context
}

func NewClientHandle(conn io.ReadWriteCloser, db *bitcask.BitcaskDB) *ClientHandle {
	return &ClientHandle{
		conn: conn,
		db:   db,
		// ctx:  ctx,
	}
}

func (cli *ClientHandle) Handle() {
	defer cli.close()

	for {
		buffer := make([]byte, cmdBufferSize)
		n, err := cli.conn.Read(buffer)

		if err != nil {
			log.Errorf("conn read err : %v", err)
			return
		}
		log.Infof("receive cmd : %s", buffer[:n])

		// The command format is [cmd] [key/value]...
		parts := bytes.Split(bytes.TrimSpace(buffer[:n]), []byte(" "))
		command, args := bytes.ToLower(parts[0]), parts[1:]
		cmdFunc, ok := supportedCommands[string(command)]
		if !ok {
			cli.conn.Write([]byte(fmt.Sprintf("ERR unknown command '%s'", command)))
			continue
		}

		if string(command) == "QUIT" {
			return
		}

		if res, err := cmdFunc(cli, args); err != nil {
			if err == bitcask.ErrKeyNotFound {
				cli.conn.Write(nil)
			} else {
				cli.conn.Write([]byte(err.Error()))
			}
		} else {
			// 通过反射判断数据类型，再统一转成[]byte形式？
			// fmt.Println("res : ", res)
			cli.conn.Write([]byte(res.(string)))
		}
	}
}

func (cli *ClientHandle) close() {
	cli.conn.Close()
}
