package client

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	"context"
)

type cmdHandler func(client *Client, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{

	// string commands
	// "set":      set,
	// "get":      get,
	// "mget":     mGet,
	// "getrange": getRange,
	// "getdel":   getDel,
	// "setex":    setEX,
	// "setnx":    setNX,
	// "mset":     mSet,
	// "msetnx":   mSetNX,
	// "append":   appendStr,
	// "decr":     decr,
	// "decrby":   decrBy,
	// "incr":     incr,
	// "incrby":   incrBy,
	// "strlen":   strLen,

	// // list
	// "lpush":  lPush,
	// "lpushx": lPushX,
	// "rpush":  rPush,
	// "rpushx": rPushX,
	// "lpop":   lPop,
	// "rpop":   rPop,
	// "lmove":  lMove,
	// "llen":   lLen,
	// "lindex": lIndex,
	// "lset":   lSet,
	// "lrange": lRange,

	// // hash commands
	// "hset":    hSet,
	// "hsetnx":  hSetNX,
	// "hget":    hGet,
	// "hmget":   hmGet,
	// "hdel":    hDel,
	// "hexists": hExists,
	// "hlen":    hLen,
	// "hkeys":   hKeys,
	// "hvals":   hVals,
	// "hgetall": hGetAll,
	// "hstrlen": hStrLen,
	// "hscan":   hScan,
	// "hincrby": hIncrBy,

	// // set commands
	// "sadd":      sAdd,
	// "spop":      sPop,
	// "srem":      sRem,
	// "sismember": sIsMember,
	// "smembers":  sMembers,
	// "scard":     sCard,
	// "sdiff":     sDiff,
	// "sunion":    sUnion,

	// // zset commands
	// "zadd":      zAdd,
	// "zscore":    zScore,
	// "zrem":      zRem,
	// "zcard":     zCard,
	// "zrange":    zRange,
	// "zrevrange": zRevRange,
	// "zrank":     zRank,
	// "zrevrank":  zRevRank,

	// // generic commands
	// "type": keyType,
	// "del":  del,

	// // connection management commands
	// "select": selectDB,
	// "ping":   ping,
	"quit":    quit,
	"slaveof": slaveof,

	// // server management commands
	"info": info,
}

func info(client *Client, args [][]byte) (interface{}, error) {
	if len(args) != 0 {
		return nil, errno.NewErr(errno.ErrWrongArgsNumber, &errno.ErrInfo{Cmd: "info"})
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

func slaveof(client *Client, args [][]byte) (interface{}, error) {
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

func quit(client *Client, args [][]byte) (interface{}, error) {
	client.Done <- struct{}{}
	return "quit....", nil
}
