package server

import (
	"bitcaskDB/internal/util"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	resultOK   = "OK"
	resultPong = "PONG"
)

var (
	errSyntax            = errors.New("ERR syntax error ")
	errValueIsInvalid    = errors.New("ERR value is not an integer or out of range")
	errDBIndexOutOfRange = errors.New("ERR DB index is out of range")
)

type cmdHandler func(cli *ClientHandle, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	// string commands
	"set":      set,
	"get":      get,
	"mget":     mGet,
	"getrange": getRange,
	"getdel":   getDel,
	"setex":    setEX,
	"setnx":    setNX,
	"mset":     mSet,
	"msetnx":   mSetNX,
	"append":   appendStr,
	"decr":     decr,
	"decrby":   decrBy,
	"incr":     incr,
	"incrby":   incrBy,
	"strlen":   strLen,

	// list
	"lpush":  lPush,
	"lpushx": lPushX,
	"rpush":  rPush,
	"rpushx": rPushX,
	"lpop":   lPop,
	"rpop":   rPop,
	"lmove":  lMove,
	"llen":   lLen,
	"lindex": lIndex,
	"lset":   lSet,
	"lrange": lRange,

	// hash commands
	"hset":    hSet,
	"hsetnx":  hSetNX,
	"hget":    hGet,
	"hmget":   hmGet,
	"hdel":    hDel,
	"hexists": hExists,
	"hlen":    hLen,
	"hkeys":   hKeys,
	"hvals":   hVals,
	"hgetall": hGetAll,
	"hstrlen": hStrLen,
	"hscan":   hScan,
	"hincrby": hIncrBy,

	// set commands
	"sadd":      sAdd,
	"spop":      sPop,
	"srem":      sRem,
	"sismember": sIsMember,
	"smembers":  sMembers,
	"scard":     sCard,
	"sdiff":     sDiff,
	"sunion":    sUnion,

	// zset commands
	"zadd":      zAdd,
	"zscore":    zScore,
	"zrem":      zRem,
	"zcard":     zCard,
	"zrange":    zRange,
	"zrevrange": zRevRange,
	"zrank":     zRank,
	"zrevrank":  zRevRank,

	// generic commands
	"type": keyType,
	"del":  del,

	// connection management commands
	"select": selectDB,
	"ping":   ping,
	"quit":   nil,

	// server management commands
	"info": info,
}

func newWrongNumOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------------- String commands --------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func set(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("SET")
	}

	var setErr error
	key, value := args[0], args[1]
	if len(args) > 2 {
		if len(args) != 4 || strings.ToLower(string(args[2])) != "ex" {
			return nil, errSyntax
		}
		second, err := strconv.Atoi(string(args[3]))
		if err != nil {
			return nil, errSyntax
		}
		setErr = cli.db.SetEX(key, value, time.Second*time.Duration(second))
	} else {
		setErr = cli.db.Set(key, value)
	}
	if setErr != nil {
		return nil, setErr
	}
	return resultOK, nil
}

func get(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("get")
	}
	value, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}
	return value, nil
}

func mGet(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, newWrongNumOfArgsError("mget")
	}
	values, err := cli.db.MGet(args)
	return values, err
}

func getRange(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func getDel(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("getdel")
	}
	return cli.db.GetDel(args[0])
}

func setEX(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumOfArgsError("setEX")
	}
	key, seconds, value := args[0], args[1], args[2]
	sec, err := strconv.Atoi(string(seconds))
	if err != nil {
		return nil, errValueIsInvalid
	}
	err = cli.db.SetEX(key, value, time.Second*time.Duration(sec))
	if err != nil {
		return nil, err
	}
	return resultOK, nil
}

func setNX(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("setnx")
	}
	key, value := args[0], args[1]
	if err := cli.db.SetNX(key, value); err != nil {
		return nil, err
	}
	return resultOK, nil
}

func mSet(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return nil, newWrongNumOfArgsError("mset")
	}
	if err := cli.db.MSet(args...); err != nil {
		return nil, err
	}
	return resultOK, nil
}

func mSetNX(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return nil, newWrongNumOfArgsError("msetnx")
	}
	if err := cli.db.MSetNX(args...); err != nil {
		return nil, err
	}
	return resultOK, nil
}

func appendStr(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("append")
	}
	if err := cli.db.Append(args[0], args[1]); err != nil {
		return nil, err
	}
	return resultOK, nil
}

func decr(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("decr")
	}
	key := args[0]
	return cli.db.Decr(key)
}

func decrBy(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("decrby")
	}
	key, decrVal := args[0], args[1]
	decrInt64, err := util.StrToInt64(string(decrVal))
	if err != nil {
		return nil, err
	}
	return cli.db.DecrBy(key, decrInt64)
}

func incr(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("incr")
	}
	key := args[0]
	return cli.db.Incr(key)
}

func incrBy(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("incrby")
	}
	key, decrVal := args[0], args[1]
	decrInt64, err := util.StrToInt64(string(decrVal))
	if err != nil {
		return nil, err
	}
	return cli.db.IncrBy(key, decrInt64)
}

func strLen(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("strlen")
	}
	return cli.db.StrLen(args[0]), nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------------- List commands ---------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func lPush(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func lPushX(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func rPush(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func rPushX(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func lPop(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func rPop(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func lMove(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func lLen(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func lIndex(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func lSet(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func lRange(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |--------------------------- Hash commands ----------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func hSet(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func hSetNX(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func hGet(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func hmGet(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func hDel(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func hExists(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func hLen(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func hKeys(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func hVals(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func hGetAll(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func hStrLen(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func hScan(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func hIncrBy(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------------- Set commands ----------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func sAdd(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func sRem(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func sPop(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func sIsMember(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func sMembers(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func sCard(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func sDiff(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func sUnion(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------- Sorted Set commands ------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+

func zAdd(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func zScore(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func zRem(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func zCard(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func zRange(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func zRevRange(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func zRank(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func zRevRank(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------------- generic commands --------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func del(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func keyType(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// todo
	return "string", nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------- server management commands --------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func info(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// todo
	return "info", nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------- connection management commands ------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func selectDB(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// todo
	return resultOK, nil
}

func ping(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) > 1 {
		return nil, newWrongNumOfArgsError("ping")
	}
	var res = resultPong
	if len(args) == 1 {
		res = string(args[0])
	}
	return res, nil
}
