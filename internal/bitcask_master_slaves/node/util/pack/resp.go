package pack

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	"fmt"
	"reflect"
	"time"
)

type PackTypeCode int8

const (
	OpLogEntryResp PackTypeCode = iota
)

func BuildResp(code PackTypeCode, data interface{}) (interface{}, error) {
	switch code {
	case OpLogEntryResp:
		return BuildOpLogEntryResp(data)
	default:
		return BuildOpLogEntryResp(data)
	}
}

func BuildOpLogEntryResp(vi interface{}) (interface{}, error) {
	resp := &node.LogEntryResponse{}
	var info string
	typ := reflect.ValueOf(vi)
	switch typ.Kind() {
	case reflect.Slice:
		// Only consider []byte and [][]byte
		resp.Entries = ByteSliceToLogEntryArr(vi)
	case reflect.String:
		if val := vi.(string); val == "OK" || val == "PONG" {
			info = val
		} else {
			info = fmt.Sprintf("\"%s\"", vi.(string))
		}
	case reflect.Int:
		info = fmt.Sprintf("(integer) %d", vi.(int))
	case reflect.Int64:
		info = fmt.Sprintf("(integer) %d", vi.(int64))
	case reflect.Float64:
		info = fmt.Sprintf("\"%f\"", vi.(float64))
	case reflect.Invalid:
		info = "(nil)"
	case reflect.Bool:
		if vi.(bool) {
			info = "(integer) 1"
		}
		info = "(integer) 0"
	default:
		info = "(undefine)"
	}

	resp.Info = info
	resp.BaseResp = BuildBaseResp(node.ErrCode_SuccessCode, nil)
	return resp, nil
}

func BuildBaseResp(code node.ErrCode, err error) *node.BaseResp {
	if err == nil {
		err = errno.Success
	}
	return &node.BaseResp{
		StatusCode:    int64(code),
		StatusMessage: err.Error(),
		ServiceTime:   time.Now().Unix()}
}

func ByteSliceToLogEntryArr(vi interface{}) []*node.LogEntry {
	res := []*node.LogEntry{}

	v := reflect.ValueOf(vi)
	l := v.Len()

	if l == 0 { // empty slice
		return res
	}
	if v.Index(0).Kind() == reflect.Uint8 {
		// []byte
		val, ok := vi.([]byte)
		if !ok {
			return res
		}
		res = append(res, &node.LogEntry{Value: string(val)})
	} else {
		// [][]byte
		// Convert to [][]byte and get the element.
		// If use v.Index() to get element and pass it to ConvertToBSlice(), it will return "(undefine)"
		// Because:
		// reflect.ValueOf(v.Index(i)).Kind() ----- struct
		// v.index(i).Kind() ------ slice
		values, ok := vi.([][]byte)
		if !ok {
			return res
		}
		for _, val := range values {
			res = append(res, &node.LogEntry{Value: string(val)})
		}
	}

	return res
}
