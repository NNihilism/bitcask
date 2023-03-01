package msClient

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/util"
	"context"
	"errors"
)

const (
	resultOK   = "OK"
	resultPong = "PONG"
)

var (
	errSyntax            = errors.New("ERR syntax error ")
	errValueIsInvalid    = errors.New("ERR value is not an integer or out of range")
	errFloatIsInvalid    = errors.New("ERR value is not a valid float")
	errDBIndexOutOfRange = errors.New("ERR DB index is out of range")
)

// type cmdHandler func (cli *Client)(args [][]byte) (interface{}, error)

// var supportedCommands = map[string]cmdHandler{
//     // string commands
//     "set":      set,
//     "get":      get,
//     "mget":     mGet,
//     "getrange": getRange,
//     "getdel":   getDel,
//     "setex":    setEX,
//     "setnx":    setNX,
//     "mset":     mSet,
//     "msetnx":   mSetNX,
//     "append":   appendStr,
//     "decr":     decr,
//     "decrby":   decrBy,
//     "incr":     incr,
//     "incrby":   incrBy,
//     "strlen":   strLen,

//     // list
//     "lpush":  lPush,
//     "lpushx": lPushX,
//     "rpush":  rPush,
//     "rpushx": rPushX,
//     "lpop":   lPop,
//     "rpop":   rPop,
//     "lmove":  lMove,
//     "llen":   lLen,
//     "lindex": lIndex,
//     "lset":   lSet,
//     "lrange": lRange,

//     // hash commands
//     "hset":    hSet,
//     "hsetnx":  hSetNX,
//     "hget":    hGet,
//     "hmget":   hmGet,
//     "hdel":    hDel,
//     "hexists": hExists,
//     "hlen":    hLen,
//     "hfields": hFields,
//     "hvals":   hVals,
//     "hgetall": hGetAll,
//     "hstrlen": hStrLen,
//     "hscan":   hScan,
//     "hincrby": hIncrBy,

//     // set commands
//     "sadd":      sAdd,
//     "spop":      sPop,
//     "srem":      sRem,
//     "sismember": sIsMember,
//     "smembers":  sMembers,
//     "scard":     sCard,
//     "sdiff":     sDiff,
//     "sunion":    sUnion,

//     // zset commands
//     "zadd":      zAdd,
//     "zscore":    zScore,
//     "zrem":      zRem,
//     "zcard":     zCard,
//     "zrange":    zRange,
//     "zrevrange": zRevRange,
//     "zrank":     zRank,
//     "zrevrank":  zRevRank,

//     // generic commands
//     "type": keyType,
//     "del":  del,

//     // connection management commands
//     "select": selectDB,
//     "ping":   ping,
//     "quit":   nil,

//     // server management commands
//     "info": info,
// }

// func newWrongNumOfArgsError(cmd string) error {
//  return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
// }

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------------- String commands --------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func (cli *Client) Set(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("set")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "set",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) Get(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("get")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "get",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) MGet(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("mget")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "mget",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) GetRange(args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func (cli *Client) GetDel(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("getdel")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "getdel",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) SetEX(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("setex")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "setex",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) SetNX(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("setnx")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "setnx",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) MSet(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("mset")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "mset",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) MSetNX(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("msetnx")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "msetnx",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) AppendStr(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("appendstr")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "appendstr",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) Decr(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("decr")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "decr",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) DecrBy(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("decrby")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "decrby",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) Incr(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("incr")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "incr",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) IncrBy(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("incrby")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "incrby",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) StrLen(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("strlen")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "strlen",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------------- List commands ---------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func (cli *Client) LPush(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("lpush")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "lpush",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) LPushX(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("lpushx")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "lpushx",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) RPush(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("rpush")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "rpush",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) RPushX(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("rpushx")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "rpushx",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) LPop(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("lpop")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "lpop",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) RPop(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("rpop")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "rpop",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) LMove(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("lmove")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "lmove",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) LLen(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("llen")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "llen",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) LIndex(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("lindex")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "lindex",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) LSet(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("lset")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "lset",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) LRange(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("lrange")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "lrange",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |--------------------------- Hash commands ----------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func (cli *Client) HSet(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("hset")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "hset",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) HSetNX(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("hsetnx")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "hsetnx",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) HGet(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("hget")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "hget",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) HmGet(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("hmget")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "hmget",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) HDel(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("hdel")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "hdel",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) HExists(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("hexists")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "hexists",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) HLen(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("hlen")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "hlen",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) HFields(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("hfields")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "hfields",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) HVals(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("hvals")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "hvals",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) HGetAll(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("hgetall")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "hgetall",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) HStrLen(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("hstrlen")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "hstrlen",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) HScan(args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func (cli *Client) HIncrBy(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("hincrby")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "hincrby",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------------- Set commands ----------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func (cli *Client) SAdd(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("sadd")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "sadd",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) SRem(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("srem")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "srem",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) SPop(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("spop")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "spop",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) SIsMember(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("sismember")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "sismember",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) SMembers(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("smembers")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "smembers",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) SCard(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("scard")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "scard",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) SDiff(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("sdiff")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "sdiff",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) SUnion(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("sunion")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "sunion",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------- Sorted Set commands ------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+

func (cli *Client) ZAdd(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("zadd")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "zadd",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) ZScore(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("zscore")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "zscore",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) ZRem(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("zrem")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "zrem",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) ZCard(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("zcard")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "zcard",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) ZRange(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("zrange")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "zrange",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) ZRevRange(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("zrevrange")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "zrevrange",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) ZRank(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("zrank")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "zrank",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

func (cli *Client) ZRevRank(args [][]byte) (interface{}, error) {
	rpc := cli.getRpc("zrevrank")
	resp, err := rpc.OpLogEntry(context.Background(), &node.LogEntryRequest{
		Cmd:   "zrevrank",
		Args_: util.BytesArrToStrArr(args),
	})
	if err != nil {
		return nil, err
	}
	if resp.BaseResp.StatusCode == 0 {
		if len(resp.Entries) == 0 {
			return resp.Info, nil
		}
		return resp.Entries, nil
	}
	return nil, nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------------- generic commands --------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------- server management commands --------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func (cli *Client) Info(args [][]byte) (interface{}, error) {
	// todo
	return "info", nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------- connection management commands ------------------|
// +-------+--------+----------+------------+-----------+-------+---------+

func (cli *Client) ping(args [][]byte) (interface{}, error) {
	// if len(args) > 1 {
	//     return nil, newWrongNumOfArgsError("ping")
	// }
	// var res = resultPong
	// if len(args) == 1 {
	//     res = string(args[0])
	// }
	// return res, nil
	return nil, nil
}
