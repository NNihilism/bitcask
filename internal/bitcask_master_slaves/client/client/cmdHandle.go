package client

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	"bitcaskDB/internal/util"
	"context"
)

type cmdHandler func(client *Client, cmd []byte, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{

	// string commands
	"set":      opLogEntry,
	"get":      opLogEntry,
	"mget":     opLogEntry,
	"getrange": opLogEntry,
	"getdel":   opLogEntry,
	"setex":    opLogEntry,
	"setnx":    opLogEntry,
	"mset":     opLogEntry,
	"msetnx":   opLogEntry,
	"append":   opLogEntry,
	"decr":     opLogEntry,
	"decrby":   opLogEntry,
	"incr":     opLogEntry,
	"incrby":   opLogEntry,
	"strlen":   opLogEntry,

	// list
	"lpush":  opLogEntry,
	"lpushx": opLogEntry,
	"rpush":  opLogEntry,
	"rpushx": opLogEntry,
	"lpop":   opLogEntry,
	"rpop":   opLogEntry,
	"lmove":  opLogEntry,
	"llen":   opLogEntry,
	"lindex": opLogEntry,
	"lset":   opLogEntry,
	"lrange": opLogEntry,

	// hash commands
	"hset":    opLogEntry,
	"hsetnx":  opLogEntry,
	"hget":    opLogEntry,
	"hmget":   opLogEntry,
	"hdel":    opLogEntry,
	"hexists": opLogEntry,
	"hlen":    opLogEntry,
	"hkeys":   opLogEntry,
	"hvals":   opLogEntry,
	"hgetall": opLogEntry,
	"hstrlen": opLogEntry,
	"hscan":   opLogEntry,
	"hincrby": opLogEntry,

	// set commands
	"sadd":      opLogEntry,
	"spop":      opLogEntry,
	"srem":      opLogEntry,
	"sismember": opLogEntry,
	"smembers":  opLogEntry,
	"scard":     opLogEntry,
	"sdiff":     opLogEntry,
	"sunion":    opLogEntry,

	// zset commands
	"zadd":      opLogEntry,
	"zscore":    opLogEntry,
	"zrem":      opLogEntry,
	"zcard":     opLogEntry,
	"zrange":    opLogEntry,
	"zrevrange": opLogEntry,
	"zrank":     opLogEntry,
	"zrevrank":  opLogEntry,

	// // generic commands
	// "type": keyType,
	// "del":  del,

	// // connection management commands
	// "select": selectDB,
	"ping":    opLogEntry,
	"quit":    quit,
	"slaveof": slaveof,

	// // server management commands
	"info": info,
}

func info(client *Client, cmd []byte, args [][]byte) (interface{}, error) {
	if len(args) != 0 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "info"})
	}
	resp, err := client.rpcClient.Info(context.Background())

	if err != nil {
		return nil, err
	}
	if result, err := ToString(resp); err != nil {
		return nil, err
	} else {
		return result, nil
	}
}

func slaveof(client *Client, cmd []byte, args [][]byte) (interface{}, error) {
	resp, err := client.rpcClient.SendSlaveof(context.Background(), &node.SendSlaveofRequest{
		Address: string(args[0]),
	})
	if err != nil {
		return nil, err
	}
	if result, err := ToString(resp); err != nil {
		return nil, err
	} else {
		return result, nil
	}
}

func quit(client *Client, cmd []byte, args [][]byte) (interface{}, error) {
	client.Done <- struct{}{}
	return "quit....", nil
}

func opLogEntry(client *Client, cmd []byte, args [][]byte) (interface{}, error) {
	resp, err := client.rpcClient.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   string(cmd),
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if result, err := ToString(resp); err != nil {
		return nil, err
	} else {
		return result, nil
	}
}
