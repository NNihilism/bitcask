package server

import (
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
	"get":      nil,
	"mget":     nil,
	"getrange": nil,
	"getdel":   nil,
	"setex":    setEX,
	"setnx":    nil,
	"mset":     nil,
	"msetnx":   nil,
	"append":   nil,
	"decr":     nil,
	"decrby":   nil,
	"incr":     nil,
	"incrby":   nil,
	"strlen":   nil,

	// list
	"lpush":  nil,
	"lpushx": nil,
	"rpush":  nil,
	"rpushx": nil,
	"lpop":   nil,
	"rpop":   nil,
	"lmove":  nil,
	"llen":   nil,
	"lindex": nil,
	"lset":   nil,
	"lrange": nil,

	// hash commands
	"hset":    nil,
	"hsetnx":  nil,
	"hget":    nil,
	"hmget":   nil,
	"hdel":    nil,
	"hexists": nil,
	"hlen":    nil,
	"hkeys":   nil,
	"hvals":   nil,
	"hgetall": nil,
	"hstrlen": nil,
	"hscan":   nil,
	"hincrby": nil,

	// set commands
	"sadd":      nil,
	"spop":      nil,
	"srem":      nil,
	"sismember": nil,
	"smembers":  nil,
	"scard":     nil,
	"sdiff":     nil,
	"sunion":    nil,

	// zset commands
	"zadd":      nil,
	"zscore":    nil,
	"zrem":      nil,
	"zcard":     nil,
	"zrange":    nil,
	"zrevrange": nil,
	"zrank":     nil,
	"zrevrank":  nil,

	// generic commands
	"type": nil,
	"del":  nil,

	// connection management commands
	"select": nil,
	"ping":   nil,
	"quit":   nil,

	// server management commands
	"info": nil,
}

func newWrongNumOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

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
