package server

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/util"
	"bytes"
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
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("lpush")
	}
	if err := cli.db.LPush(args[0], args[1:]...); err != nil {
		return nil, err
	}
	return cli.db.LLen(args[0]), nil
}

func lPushX(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("lpush")
	}
	if err := cli.db.LPushX(args[0], args[1:]...); err != nil && err != bitcask.ErrKeyNotFound {
		return nil, err
	}
	return cli.db.LLen(args[0]), nil
}

func rPush(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("lpush")
	}
	if err := cli.db.RPush(args[0], args[1:]...); err != nil {
		return nil, err
	}
	return cli.db.LLen(args[0]), nil
}

func rPushX(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("lpush")
	}
	if err := cli.db.LPushX(args[0], args[1:]...); err != nil && err != bitcask.ErrKeyNotFound {
		return nil, err
	}
	return cli.db.LLen(args[0]), nil
}

func lPop(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("lpop")
	}
	if val, err := cli.db.LPop(args[0]); err != nil {
		return nil, err
	} else {
		return val, nil
	}
}

func rPop(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("lpop")
	}
	if val, err := cli.db.RPop(args[0]); err != nil {
		return nil, err
	} else {
		return val, nil
	}
}

func lMove(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 4 {
		return nil, newWrongNumOfArgsError("lmove")
	}
	srcKey, dstKey := args[0], args[1]
	from, to := bytes.ToLower(args[2]), bytes.ToLower(args[3])
	var srcIsLeft, dstIsLeft bool

	if string(from) == "left" {
		srcIsLeft = true
	} else if string(from) != "right" {
		return nil, errSyntax
	}
	if string(to) == "left" {
		dstIsLeft = true
	} else if string(to) != "right" {
		return nil, errSyntax
	}

	return cli.db.LMove(srcKey, dstKey, srcIsLeft, dstIsLeft)
}

func lLen(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("llen")
	}
	return cli.db.LLen(args[0]), nil
}

func lIndex(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("lindex")
	}
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, err
	}
	return cli.db.LIndex(args[0], index)
}

func lSet(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumOfArgsError("lset")
	}
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, err
	}
	if err := cli.db.LSet(args[0], index, args[2]); err != nil {
		return nil, err
	}
	return resultOK, nil
}

func lRange(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumOfArgsError("lrange")
	}
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, err
	}
	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return nil, err
	}
	return cli.db.LRange(args[0], start, stop)
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |--------------------------- Hash commands ----------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func hSet(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) < 2 || len(args)%2 == 0 {
		return nil, newWrongNumOfArgsError("hset")
	}
	key := args[0]
	var cnt int
	for i := 1; i < len(args); i += 2 {
		err := cli.db.HSet(key, args[i], args[i+1])
		if err != nil {
			return nil, err
		}
		cnt++
	}
	return cnt, nil
}

func hSetNX(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumOfArgsError("hsetnx")
	}
	if ok, err := cli.db.HSetNX(args[0], args[1], args[2]); err != nil {
		return nil, err
	} else if ok {
		return 1, nil
	} else {
		return 0, nil
	}
}

func hGet(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("hget")
	}
	return cli.db.HGet(args[0], args[1])
}

func hmGet(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("hmget")
	}
	return cli.db.HMGet(args[0], args[1:]...)
}

func hDel(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, newWrongNumOfArgsError("hdel")
	}
	return cli.db.HDel(args[0], args[1:]...)
}

func hExists(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("hdel")
	}
	return cli.db.HExists(args[0], args[1])
}

func hLen(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("hlen")
	}
	return cli.db.HLen(args[0]), nil
}

func hKeys(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("hkeys")
	}
	return cli.db.HKeys(args[0])
}

func hVals(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("hvals")
	}
	return cli.db.HVals(args[0])
}

func hGetAll(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumOfArgsError("hgetall")
	}
	return cli.db.HGetAll(args[0])
}

func hStrLen(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumOfArgsError("hstrlen")
	}
	return cli.db.HStrLen(args[0], args[1]), nil
}

func hScan(cli *ClientHandle, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func hIncrBy(cli *ClientHandle, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumOfArgsError("hincrby")
	}
	incr, err := util.StrToInt64(string(args[2]))
	if err != nil {
		return nil, errValueIsInvalid
	}
	return cli.db.HIncrBy(args[0], args[1], incr)
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
