package cmd

import (
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	prxyservice "bitcaskDB/internal/bitcask_master_slaves/proxy/kitex_gen/prxyService"
	"bytes"
	"errors"
	"reflect"
	"strconv"
)

func ToString(obj interface{}) ([]byte, error) {
	typeOfObj := reflect.TypeOf(obj).Elem()
	var bf bytes.Buffer

	switch typeOfObj.Name() {
	case "LogEntryResponse":
		// type LogEntryResponse struct {
		// 	BaseResp *BaseResp   `thrift:"base_resp,1" frugal:"1,default,BaseResp" json:"base_resp"`
		// 	Entries  []*LogEntry `thrift:"entries,2" frugal:"2,default,list<LogEntry>" json:"entries"`
		// 	Info     string      `thrift:"info,3" frugal:"3,default,string" json:"info"`
		// }
		resp := obj.(*prxyservice.LogEntryResponse)
		if resp.BaseResp.StatusCode != 0 {
			return nil, errors.New(resp.BaseResp.StatusMessage)
		}
		if resp.Info != "" {
			return []byte(resp.Info), nil
		}
		for i, entry := range resp.Entries {
			bf.WriteString(strconv.Itoa(i) + ") ")
			bf.WriteString(entry.Value)
			if i != len(resp.Entries) {
				bf.WriteString("\n")
			}
		}
	default:
		return []byte{}, errno.NewErr(errno.ErrCodeParseResp, &errno.ErrInfo{Obj: obj})
	}

	return bf.Bytes(), nil

}
