package nodeCore

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"context"
	"fmt"
	"sync"
)

// 异步更新
func (bitcaskNode *BitcaskNode) AsynchronousSync(req *node.LogEntryRequest) {
	for _, rpc := range bitcaskNode.slavesRpc {
		ctx, _ := context.WithTimeout(context.Background(), config.RpcTimeOut)
		rpc.OpLogEntry(ctx, req)
	}
}

// 半同步更新
func (bitcaskNode *BitcaskNode) SemiSynchronousSync(req *node.LogEntryRequest) {
	wg := new(sync.WaitGroup)
	semi_cnt := int(float64(len(bitcaskNode.slavesRpc)) * config.SemiSynchronousRate)
	wg.Add(semi_cnt)

	for _, rpc := range bitcaskNode.slavesRpc {
		go func(rpc nodeservice.Client) {
			defer wg.Done()
			ctx, _ := context.WithTimeout(context.Background(), config.RpcTimeOut)
			rpc.OpLogEntry(ctx, req)
		}(rpc)
	}

	wg.Wait()
}

// 同步更新
func (bitcaskNode *BitcaskNode) SynchronousSync(req *node.LogEntryRequest) {
	for _, rpc := range bitcaskNode.slavesRpc {
		ctx, _ := context.WithTimeout(context.Background(), config.RpcTimeOut)
		rpc.OpLogEntry(ctx, req)
	}
}

// 全量复制
func (bitcaskNode *BitcaskNode) FullReplication(slaveId int64) {

}

// 增量复制
func (bitcakNode *BitcaskNode) IncreReplication(slaveId int64, offset int64) {

}

// 主节点收到PSync后，判断进行增量复制还是全量复制，将判断结果返回，并异步执行复制
func (bitcaskNode *BitcaskNode) HandlePSyncReq(req *node.PSyncRequest) (*node.PSyncResponse, error) {
	slave_repl_offset := req.Offset

	resp := new(node.PSyncResponse)
	// 如果缓存中能找到slave节点想要的偏移量， 则增量复制
	if _, ok := bitcaskNode.opCache.Get(fmt.Sprintf("%d", slave_repl_offset)); ok {
		//
		// go 增量复制
		resp.Code = 1 //这里写死了，命名困难
	} else {
		// go 全量复制
		resp.Code = 2
	}
	return resp, nil
}
