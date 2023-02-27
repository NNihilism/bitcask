package nodeCore

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"context"
	"sync"
)

func (bitcaskNode *BitcaskNode) AsynchronousSync(req *node.LogEntryRequest) {
	for _, rpc := range bitcaskNode.slavesRpc {
		ctx, _ := context.WithTimeout(context.Background(), config.RpcTimeOut)
		rpc.OpLogEntry(ctx, req)
	}
}

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

func (bitcaskNode *BitcaskNode) SynchronousSync(req *node.LogEntryRequest) {
	for _, rpc := range bitcaskNode.slavesRpc {
		ctx, _ := context.WithTimeout(context.Background(), config.RpcTimeOut)
		rpc.OpLogEntry(ctx, req)
	}
}
