package nodeCore

import (
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	"strconv"
)

// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------- Sorted Set commands ------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+

func zAdd(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "zadd"})
	}
	score, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return 0, errno.ErrFloatIsInvalid
	}
	if err := bitcaskNode.db.ZAdd(args[0], score, args[2]); err != nil {
		return nil, err
	}
	return 1, nil
}

func zScore(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "zscore"})
	}
	if ok, score := bitcaskNode.db.ZScore(args[0], args[1]); !ok {
		return nil, nil
	} else {
		return score, nil
	}
}

func zRem(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "zrem"})
	}
	if err := bitcaskNode.db.ZRem(args[0], args[1]); err != nil {
		return false, err
	}
	return true, nil
}

func zCard(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "zcard"})
	}
	return bitcaskNode.db.ZCard(args[0]), nil
}

func zRange(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "zrange"})
	}

	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, errno.ErrValueIsInvalid
	}

	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return nil, errno.ErrValueIsInvalid
	}
	return bitcaskNode.db.ZRange(args[0], start, stop)
}

func zRevRange(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "zrevrange"})
	}

	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return nil, errno.ErrValueIsInvalid
	}

	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return nil, errno.ErrValueIsInvalid
	}
	return bitcaskNode.db.ZRevRange(args[0], start, stop)
}

func zRank(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "zrank"})
	}
	if ok, rank := bitcaskNode.db.ZRank(args[0], args[1]); ok {
		return rank, nil
	}
	return nil, nil
}

func zRevRank(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "zrevrank"})
	}
	if ok, rank := bitcaskNode.db.ZRevRank(args[0], args[1]); ok {
		return rank, nil
	}
	return nil, nil
}
