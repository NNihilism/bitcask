package nodeCore

import (
	"bitcaskDB/internal/bitcask"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	"bytes"
	"strconv"
)

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------------- List commands ---------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+

func lPush(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "lpush"})
	}
	if err := bitcaskNode.db.LPush(args[0], args[1:]...); err != nil {
		return nil, err
	}
	return bitcaskNode.db.LLen(args[0]), nil
}

func lPushX(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "lpush"})
	}
	if err := bitcaskNode.db.LPushX(args[0], args[1:]...); err != nil && err != bitcask.ErrKeyNotFound {
		return nil, err
	}
	return bitcaskNode.db.LLen(args[0]), nil
}

func rPush(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "lpush"})
	}
	if err := bitcaskNode.db.RPush(args[0], args[1:]...); err != nil {
		return nil, err
	}
	return bitcaskNode.db.LLen(args[0]), nil
}

func rPushX(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "lpush"})
	}
	if err := bitcaskNode.db.LPushX(args[0], args[1:]...); err != nil && err != bitcask.ErrKeyNotFound {
		return nil, err
	}
	return bitcaskNode.db.LLen(args[0]), nil
}

func lPop(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "lpop"})
	}
	if val, err := bitcaskNode.db.LPop(args[0]); err != nil {
		return nil, err
	} else {
		return val, nil
	}
}

func rPop(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "lpop"})
	}
	if val, err := bitcaskNode.db.RPop(args[0]); err != nil {
		return nil, err
	} else {
		return val, nil
	}
}

func lMove(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 4 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "lmove"})
	}
	srcKey, dstKey := args[0], args[1]
	from, to := bytes.ToLower(args[2]), bytes.ToLower(args[3])
	var srcIsLeft, dstIsLeft bool

	if string(from) == "left" {
		srcIsLeft = true
	} else if string(from) != "right" {
		return nil, errno.ErrSyntax
	}
	if string(to) == "left" {
		dstIsLeft = true
	} else if string(to) != "right" {
		return nil, errno.ErrSyntax
	}

	return bitcaskNode.db.LMove(srcKey, dstKey, srcIsLeft, dstIsLeft)
}

func lLen(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "llen"})
	}
	return bitcaskNode.db.LLen(args[0]), nil
}

func lIndex(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "lindex"})
	}
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, errno.ErrValueIsInvalid
	}
	return bitcaskNode.db.LIndex(args[0], index)
}

func lSet(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "lset"})
	}
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, errno.ErrValueIsInvalid
	}
	if err := bitcaskNode.db.LSet(args[0], index, args[2]); err != nil {
		return nil, err
	}
	return resultOK, nil
}

func lRange(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "lrange"})
	}
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, errno.ErrValueIsInvalid
	}
	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return nil, errno.ErrValueIsInvalid
	}
	return bitcaskNode.db.LRange(args[0], start, stop)
}
