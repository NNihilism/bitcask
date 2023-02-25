package client

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	"bytes"
	"errors"
	"reflect"
	"strconv"
)

func ToString(obj interface{}) ([]byte, error) {
	typeOfObj := reflect.TypeOf(obj).Elem()
	var bf bytes.Buffer

	switch typeOfObj.Name() {
	case "InfoResponse":
		// type InfoResponse struct {
		// 	Role                    string `thrift:"role,1" frugal:"1,default,string" json:"role"`
		// 	ConnectedSlaves         int64  `thrift:"connected_slaves,2" frugal:"2,default,i64" json:"connected_slaves"`
		// 	MasterReplicationOffset int64  `thrift:"master_replication_offset,3" frugal:"3,default,i64" json:"master_replication_offset"`
		// 	CurReplicationOffset    int64  `thrift:"cur_replication_offset,4" frugal:"4,default,i64" json:"cur_replication_offset"`
		// }
		resp := obj.(*node.InfoResponse)

		bf.WriteString("role:")
		bf.WriteString(resp.Role)
		bf.WriteString("\n")

		bf.WriteString("ConnectedSlaves:")
		bf.WriteString(strconv.Itoa(int(resp.ConnectedSlaves)))
		bf.WriteString("\n")

		bf.WriteString("MasterReplicationOffset:")
		bf.WriteString(strconv.Itoa(int(resp.MasterReplicationOffset)))
		bf.WriteString("\n")

		bf.WriteString("CurReplicationOffset:")
		bf.WriteString(strconv.Itoa(int(resp.CurReplicationOffset)))
		bf.WriteString("\n")
	case "SendSlaveofResponse":
		// type SendSlaveofResponse struct {
		// 	BaseResp *BaseResp `thrift:"base_resp,1" frugal:"1,default,BaseResp" json:"base_resp"`
		// }
		// type BaseResp struct {
		// 	StatusCode    int64  `thrift:"status_code,1" frugal:"1,default,i64" json:"status_code"`
		// 	StatusMessage string `thrift:"status_message,2" frugal:"2,default,string" json:"status_message"`
		// 	ServiceTime   int64  `thrift:"service_time,3" frugal:"3,default,i64" json:"service_time"`
		// }
		resp := obj.(*node.SendSlaveofResponse)
		if resp.BaseResp.StatusCode != 0 {
			// bf.WriteString("")
			return nil, errors.New(resp.BaseResp.StatusMessage)
		}
		bf.WriteString("slaveof success!")
	default:
		return []byte{}, errno.NewErr(errno.ErrParseResp, &errno.ErrInfo{Obj: obj})

	}
	// if typeOfObj.Name() == "InfoResponse" {
	// 	// type InfoResponse struct {
	// 	// 	Role                    string `thrift:"role,1" frugal:"1,default,string" json:"role"`
	// 	// 	ConnectedSlaves         int64  `thrift:"connected_slaves,2" frugal:"2,default,i64" json:"connected_slaves"`
	// 	// 	MasterReplicationOffset int64  `thrift:"master_replication_offset,3" frugal:"3,default,i64" json:"master_replication_offset"`
	// 	// 	CurReplicationOffset    int64  `thrift:"cur_replication_offset,4" frugal:"4,default,i64" json:"cur_replication_offset"`
	// 	// }

	// 	resp := obj.(*node.InfoResponse)

	// 	bf.WriteString("role:")
	// 	bf.WriteString(resp.Role)
	// 	bf.WriteString("\n")

	// 	bf.WriteString("ConnectedSlaves:")
	// 	bf.WriteString(strconv.Itoa(int(resp.ConnectedSlaves)))
	// 	bf.WriteString("\n")

	// 	bf.WriteString("MasterReplicationOffset:")
	// 	bf.WriteString(strconv.Itoa(int(resp.MasterReplicationOffset)))
	// 	bf.WriteString("\n")

	// 	bf.WriteString("CurReplicationOffset:")
	// 	bf.WriteString(strconv.Itoa(int(resp.CurReplicationOffset)))
	// 	bf.WriteString("\n")
	// } else {
	// 	return []byte{}, errno.NewErr(errno.ErrParseResp, &errno.ErrInfo{Obj: obj})
	// }

	return bf.Bytes(), nil

}
