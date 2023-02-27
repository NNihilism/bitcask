package nodeCore

import (
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	"bitcaskDB/internal/util"
)

// +-------+--------+----------+------------+-----------+-------+---------+
// |--------------------------- Hash commands ----------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func hSet(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) < 2 || len(args)%2 == 0 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "hset"})
	}
	key := args[0]
	var cnt int
	for i := 1; i < len(args); i += 2 {
		err := bitcaskNode.db.HSet(key, args[i], args[i+1])
		if err != nil {
			return nil, err
		}
		cnt++
	}
	return cnt, nil
}

func hSetNX(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "hsetnx"})
	}
	if ok, err := bitcaskNode.db.HSetNX(args[0], args[1], args[2]); err != nil {
		return nil, err
	} else if ok {
		return 1, nil
	} else {
		return 0, nil
	}
}

func hGet(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "hget"})
	}
	return bitcaskNode.db.HGet(args[0], args[1])
}

func hmGet(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "hmget"})
	}
	return bitcaskNode.db.HMGet(args[0], args[1:]...)
}

func hDel(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "hdel"})
	}
	return bitcaskNode.db.HDel(args[0], args[1:]...)
}

func hExists(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "hdel"})
	}
	return bitcaskNode.db.HExists(args[0], args[1])
}

func hLen(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "hlen"})
	}
	return bitcaskNode.db.HLen(args[0]), nil
}

func hFields(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "hkeys"})
	}
	return bitcaskNode.db.HFields(args[0])
}

func hVals(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "hvals"})
	}
	return bitcaskNode.db.HVals(args[0])
}

func hGetAll(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "hgetall"})
	}
	return bitcaskNode.db.HGetAll(args[0])
}

func hStrLen(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "hstrlen"})
	}
	return bitcaskNode.db.HStrLen(args[0], args[1]), nil
}

func hScan(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func hIncrBy(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "hincrby"})
	}
	incr, err := util.StrToInt64(string(args[2]))
	if err != nil {
		return nil, errno.ErrValueIsInvalid
	}
	return bitcaskNode.db.HIncrBy(args[0], args[1], incr)
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |-------------------------- generic commands --------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func del(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	// TODO
	return resultOK, nil
}

func keyType(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	// todo
	return "string", nil
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------- server management commands --------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func info(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	// todo
	return "info", nil
}

func ping(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) > 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "ping"})
	}
	var res = "pong"
	if len(args) == 1 {
		res = string(args[0])
	}
	return res, nil
}
