package errno

import (
	"bytes"
	"fmt"
)

// type ErrNo struct {
// 	ErrCode int64
// 	ErrMsg  string
// }

type ErrCode int8

const (
	ErrWrongArgsNumber = iota
	ErrKeyNotFound
	ErrUnknownCMD
	ErrParseResp
)

type ErrInfo struct {
	Cmd  string
	Args [][]byte
	Obj  interface{}
}

func NewErr(code ErrCode, info *ErrInfo) error {
	switch code {
	case ErrWrongArgsNumber:
		return newWrongNumOfArgsError(info.Cmd)
	case ErrUnknownCMD:
		return newErrUnknownCMD(info.Cmd, info.Args)
	case ErrParseResp:
		return newErrParseResp(info.Obj)
	// case ErrKeyNotFound:
	default:
		return newWrongNumOfArgsError(info.Cmd)
	}
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

// func (e ErrNo) Error() string {
// 	return fmt.Sprintf("err_code=%d, err_msg=%s", e.ErrCode, e.ErrMsg)
// }

// func NewErrNo(code int64, msg string) ErrNo {
// 	return ErrNo{
// 		ErrCode: code,
// 		ErrMsg:  msg,
// 	}
// }

// func (e ErrNo) WithMessage(msg string) ErrNo {
// 	e.ErrMsg = msg
// 	return e
// }

// var (
// 	Success                = NewErrNo(int64(user.ErrCode_SuccessCode), "Success")
// 	ServiceErr             = NewErrNo(int64(user.ErrCode_ServiceErrCode), "Service is unable to start successfully")
// 	ParamErr               = NewErrNo(int64(user.ErrCode_ParamErrCode), "Wrong Parameter has been given")
// 	UserAlreadyExistErr    = NewErrNo(int64(user.ErrCode_UserAlreadyExistErrCode), "User already exists")
// 	AuthorizationFailedErr = NewErrNo(int64(user.ErrCode_AuthorizationFailedErrCode), "Authorization failed")
// )

// ConvertErr convert error to Errno
// func ConvertErr(err error) ErrNo {
// 	Err := ErrNo{}
// 	if errors.As(err, &Err) {
// 		return Err
// 	}
// 	s := ServiceErr
// 	s.ErrMsg = err.Error()
// 	return s
// }
