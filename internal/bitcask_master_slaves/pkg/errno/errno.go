package errno

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bytes"
	"errors"
	"fmt"
)

// type ErrNo struct {
// 	ErrCode int64
// 	ErrMsg  string
// }

type ErrCode int8

const (
	ErrCodeSuccess = iota
	ErrCodeWrongArgsNumber
	ErrCodeKeyNotFound
	ErrCodeUnknownCMD
	ErrCodeParseResp
	ErrCodeSyntax
	ErrCodeWriteOnSlave
)

var (
	Success    = NewErrNo(int64(node.ErrCode_SuccessCode), "Success")
	ServiceErr = NewErrNo(int64(node.ErrCode_ServiceErrCode), "Service is unable to start successfully")
	ParamErr   = NewErrNo(int64(node.ErrCode_ParamErrCode), "Wrong Parameter has been given")
	// UserAlreadyExistErr    = NewErrNo(int64(node.ErrCode_UserAlreadyExistErrCode), "User already exists")
	// AuthorizationFailedErr = NewErrNo(int64(node.ErrCode_AuthorizationFailedErrCode), "Authorization failed")
	ErrSyntax         = errors.New("ERR syntax error ")
	ErrValueIsInvalid = errors.New("ERR value is not an integer or out of range")

	ErrFloatIsInvalid    = errors.New("ERR value is not a valid float")
	ErrDBIndexOutOfRange = errors.New("ERR DB index is out of range")
)

type ErrNo struct {
	ErrCode int64
	ErrMsg  string
}

func (e ErrNo) Error() string {
	return fmt.Sprintf("err_code=%d, err_msg=%s", e.ErrCode, e.ErrMsg)
}

func NewErrNo(code int64, msg string) ErrNo {
	return ErrNo{
		ErrCode: code,
		ErrMsg:  msg,
	}
}

type ErrInfo struct {
	Cmd  string
	Args [][]byte
	Obj  interface{}
}

func NewErr(code ErrCode, info *ErrInfo) error {
	switch code {
	case ErrCodeWrongArgsNumber:
		return newWrongNumOfArgsError(info.Cmd)
	case ErrCodeUnknownCMD:
		return newErrUnknownCMD(info.Cmd, info.Args)
	case ErrCodeParseResp:
		return newErrParseResp(info.Obj)
	case ErrCodeSyntax:
		return newSyntaxErr()
	case ErrCodeWriteOnSlave:
		return newWriteOnSlaveErr()
	// case ErrKeyNotFound:
	default:
		return newWrongNumOfArgsError(info.Cmd)
	}
}

func newSyntaxErr() error {
	return errors.New("ERR syntax error")
}

func newWriteOnSlaveErr() error {
	return errors.New("READONLY You can't write against a read only replica")
}

func newWrongNumOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

func newErrUnknownCMD(cmd string, args [][]byte) error {
	for i, arg := range args {
		args[i] = []byte(fmt.Sprintf("'%s'", arg))
	}
	return fmt.Errorf("(error) ERR unknown command '%s', with args beginning with: %s", cmd, bytes.Join(args, []byte(", ")))
}

func newErrParseResp(obj interface{}) error {
	return fmt.Errorf("ERR parse resp [%v] err", obj)
}
