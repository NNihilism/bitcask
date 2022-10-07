package server

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/log"
	"bitcaskDB/internal/util"
	"bytes"
	"encoding/binary"
	"io"
)

const (
	CmdBufferSize = 1024
	MaxHeaderLength
)

// var (
// ErrWrongNumberOfArgs = errors.New("wrong number of arguments")
// ErrWrongNumberOfArgs = errors.New("ERR unknown command `new`, with args beginning with:")
// )

type ClientHandle struct {
	conn io.ReadWriteCloser
	db   *bitcask.BitcaskDB
	dbs  []*bitcask.BitcaskDB
}

func NewClientHandle(conn io.ReadWriteCloser, db *bitcask.BitcaskDB, dbs []*bitcask.BitcaskDB) *ClientHandle {
	return &ClientHandle{
		conn: conn,
		dbs:  dbs,
		db:   db,
	}
}

func (cli *ClientHandle) Handle() {
	defer cli.close()

	for {
		buffer := make([]byte, CmdBufferSize)
		n, err := cli.conn.Read(buffer) // block read....
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
			_, err := cli.conn.Read(buffer[n-offset:]) // 这里是否该改成非阻塞读？
			if err != nil {
				log.Errorf("conn read err : %v", err)
				return
			}
		}

		log.Infof("receive cmd : [%s]", buffer)

		// The command format is [cmd] [key/value]...
		parts := bytes.Split(bytes.TrimSpace(buffer), []byte(" "))
		command, args := bytes.ToLower(parts[0]), parts[1:]
		cmdFunc, ok := supportedCommands[string(command)]
		if !ok {
			cli.WriteResult([]byte(util.NewErrUnknownCMD(command, args).Error()))
			// cli.conn.Write([]byte(util.NewErrUnknownCMD(command, args).Error()))
			continue
		}

		if string(command) == "quit" {
			break
		}

		if res, err := cmdFunc(cli, args); err != nil {
			if err == bitcask.ErrKeyNotFound {
				cli.WriteResult([]byte("(nil)"))
				// cli.conn.Write([]byte("(nil)"))
			} else {
				cli.WriteResult([]byte("(error) " + err.Error()))

				// cli.conn.Write([]byte("(error) " + err.Error()))
			}
		} else {
			// 通过反射判断数据类型，再统一转成[]byte形式？
			// cli.conn.Write(util.ConvertToBSlice(res))
			cli.WriteResult(util.ConvertToBSlice(res))
		}
	}
}

func (cli *ClientHandle) close() {
	log.Info("close client....")
	// cli.db.Close() 好像不用关 其他用户也要用
	if err := cli.conn.Close(); err != nil {
		log.Errorf("close conn err : %v", err)
	} else {
		log.Info("close client success....")
	}
}

func (cli *ClientHandle) WriteResult(res []byte) {
	// The format of msg is [header(the length of data) + data]
	header := make([]byte, MaxHeaderLength)
	n := binary.PutUvarint(header, uint64(len(res)))
	msg := make([]byte, n+len(res))
	copy(msg, header[:n])
	copy(msg[n:], res)
	cli.conn.Write(msg)
}
