package nodeCore

import (
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	"bitcaskDB/internal/util"
)

// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------------- Set commands ----------------------------|
// +-------+--------+----------+------------+-----------+-------+---------+
func sAdd(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "sadd"})
	}
	return bitcaskNode.db.SAdd(args[0], args[1:]...)
	// return resultOK, nil
}

func sRem(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) < 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "srem"})
	}
	return bitcaskNode.db.SRem(args[0], args[1:]...)
}

func sPop(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "spop"})
	}
	count, err := util.StrToUint(string(args[1]))
	if err != nil {
		return nil, errno.ErrValueIsInvalid
	}
	return bitcaskNode.db.SPop(args[0], uint(count))
}

func sIsMember(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "sismember"})
	}
	return bitcaskNode.db.SIsMember(args[0], args[1]), nil
}

func sMembers(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "smembers"})
	}
	return bitcaskNode.db.SMembers(args[0])
}

func sCard(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "scard"})
	}
	return bitcaskNode.db.SCard(args[0]), nil
}

func sDiff(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) == 0 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "sdiff"})
	}
	return bitcaskNode.db.SDiff(args...)
}

func sUnion(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error) {
	if len(args) == 0 {
		return nil, errno.NewErr(errno.ErrCodeWrongArgsNumber, &errno.ErrInfo{Cmd: "sdiff"})
	}
	return bitcaskNode.db.SUnion(args...)
}
