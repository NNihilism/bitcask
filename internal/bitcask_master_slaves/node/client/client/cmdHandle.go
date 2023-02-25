package client

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	"bytes"
	"context"
	"reflect"
	"strconv"
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
	"quit": quit,

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

func quit(client *Client, args [][]byte) (interface{}, error) {
	client.Done <- struct{}{}
	return "quit....", nil
}

func ToString(obj interface{}) ([]byte, error) {
	typeOfObj := reflect.TypeOf(obj).Elem()
	var bf bytes.Buffer

	if typeOfObj.Name() == "InfoResponse" {
		// type InfoResponse struct {
		// 	Role                    string `thrift:"role,1" frugal:"1,default,string" json:"role"`
		// 	ConnectedSlaves         int64  `thrift:"connected_slaves,2" frugal:"2,default,i64" json:"connected_slaves"`
		// 	MasterReplicationOffset int64  `thrift:"master_replication_offset,3" frugal:"3,default,i64" json:"master_replication_offset"`
		// 	CurReplicationOffset    int64  `thrift:"cur_replication_offset,4" frugal:"4,default,i64" json:"cur_replication_offset"`
		// }

		resp := obj.(*node.InfoResponse)

		bf.WriteString("role:")
		bf.WriteString(resp.Role)
		bf.WriteString("\n")

		bf.WriteString("ConnectedSlaves:")
		bf.WriteString(strconv.Itoa(int(resp.ConnectedSlaves)))
		bf.WriteString("\n")

		bf.WriteString("MasterReplicationOffset:")
		bf.WriteString(strconv.Itoa(int(resp.MasterReplicationOffset)))
		bf.WriteString("\n")

		bf.WriteString("CurReplicationOffset:")
		bf.WriteString(strconv.Itoa(int(resp.CurReplicationOffset)))
		bf.WriteString("\n")
	} else {
		return []byte{}, errno.NewErr(errno.ErrParseResp, &errno.ErrInfo{Obj: obj})
	}

	return bf.Bytes(), nil

}
