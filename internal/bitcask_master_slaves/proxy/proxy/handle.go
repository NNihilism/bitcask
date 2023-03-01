package proxy

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	prxyservice "bitcaskDB/internal/bitcask_master_slaves/proxy/kitex_gen/prxyService"
	"bitcaskDB/internal/bitcask_master_slaves/proxy/pack"
	"context"
)

// 转发请求
func (proxy *Proxy) RedirOpReq(req *prxyservice.LogEntryRequest) (*prxyservice.LogEntryResponse, error) {
	if len(proxy.node) == 0 {
		return &prxyservice.LogEntryResponse{
			BaseResp: &prxyservice.BaseResp{
				StatusCode: 0,
			},
			Info: "no data node",
		}, nil
	}

	var resp *node.LogEntryResponse
	var err error

	if !isReadOperation(req.Cmd) {
		resp, err = proxy.masterRpc.OpLogEntry(context.Background(), pack.RePackOpLogEntryReq(req))
	} else {
		// 读请求，根据负载均衡策略选择slave节点进行发送
		slaveId := proxy.selectNode()
		resp, err = proxy.slaveRpcs[slaveId].OpLogEntry(context.Background(), pack.RePackOpLogEntryReq(req))
	}
	// 返回结果
	return pack.RePackOpLogEntryResp(resp), err
}

func isReadOperation(command string) bool {
	if _, ok := cmdReadOperationMap[command]; ok {
		return true
	}
	return false
}

var cmdReadOperationMap = map[string]struct{}{
	// string commands
	"get":      {},
	"mget":     {},
	"getrange": {},
	"strlen":   {},

	// list
	"llen":   {},
	"lindex": {},
	"lrange": {},

	// hash commands
	"hget":    {},
	"hmget":   {},
	"hexists": {},
	"hlen":    {},
	"hkeys":   {},
	"hvals":   {},
	"hgetall": {},
	"hstrlen": {},
	"hscan":   {},

	// set commands
	"sismember": {},
	"smembers":  {},
	"scard":     {},
	"sdiff":     {},
	"sunion":    {},

	// zset commands
	"zscore":    {},
	"zcard":     {},
	"zrange":    {},
	"zrevrange": {},
	"zrank":     {},
	"zrevrank":  {},
}
