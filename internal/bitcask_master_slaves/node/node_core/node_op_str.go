package nodeCore

import (
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	"bitcaskDB/internal/util"
	"strconv"
	"strings"
	"time"
)

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------------- String commands --------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func set(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "SET"})
	}

	var setErr error
	key, value := args[0], args[1]
	if len(args) > 2 {
		if len(args) != 4 || strings.ToLower(string(args[2])) != "ex" {
			return nil, errno.NewErr(errno.ErrCodeSyntax, nil)
		}
		second, err := strconv.Atoi(string(args[3]))
		if err != nil {
			return nil, errno.NewErr(errno.ErrCodeSyntax, nil)
		}
		setErr = bitcaskNode.db.SetEX(key, value, time.Second*time.Duration(second))
	} else {
		setErr = bitcaskNode.db.Set(key, value)
	}
	if setErr != nil {
		return nil, setErr
	}
	return resultOK, nil
}

func get(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "GET"})
	}
	value, err := bitcaskNode.db.Get(args[0])
	if err != nil {
		return nil, err
	}

	return value, nil
}

func mGet(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) < 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "mget"})
	}
	values, err := bitcaskNode.db.MGet(args)
	return values, err
}

func getRange(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func getDel(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "getdel"})
	}
	return bitcaskNode.db.GetDel(args[0])
}

func setEX(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "setEX"})
	}
	key, seconds, value := args[0], args[1], args[2]
	sec, err := strconv.Atoi(string(seconds))
	if err != nil {
		return nil, errno.ErrValueIsInvalid
	}
	err = bitcaskNode.db.SetEX(key, value, time.Second*time.Duration(sec))
	if err != nil {
		return nil, err
	}
	return resultOK, nil
}

func setNX(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "setNX"})
	}
	key, value := args[0], args[1]
	if err := bitcaskNode.db.SetNX(key, value); err != nil {
		return nil, err
	}
	return resultOK, nil
}

func mSet(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "mSet"})
	}
	if err := bitcaskNode.db.MSet(args...); err != nil {
		return nil, err
	}
	return resultOK, nil
}

func mSetNX(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "mSetNX"})
	}
	if err := bitcaskNode.db.MSetNX(args...); err != nil {
		return nil, err
	}
	return resultOK, nil
}

func appendStr(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "appendStr"})
	}
	if err := bitcaskNode.db.Append(args[0], args[1]); err != nil {
		return nil, err
	}
	return resultOK, nil
}

func decr(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "decr"})
	}
	key := args[0]
	return bitcaskNode.db.Decr(key)
}

func decrBy(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "decrBy"})
	}
	key, decrVal := args[0], args[1]
	decrInt64, err := util.StrToInt64(string(decrVal))
	if err != nil {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "decrBy"})
	}
	return bitcaskNode.db.DecrBy(key, decrInt64)
}

func incr(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "incr"})
	}
	key := args[0]
	return bitcaskNode.db.Incr(key)
}

func incrBy(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "incrBy"})
	}
	key, decrVal := args[0], args[1]
	decrInt64, err := util.StrToInt64(string(decrVal))
	if err != nil {
		return nil, errno.ErrValueIsInvalid
	}
	return bitcaskNode.db.IncrBy(key, decrInt64)
}

func strLen(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "strLen"})
	}
	return bitcaskNode.db.StrLen(args[0]), nil
}
