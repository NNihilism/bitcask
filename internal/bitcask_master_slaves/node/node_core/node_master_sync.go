package nodeCore

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"context"
	"time"
)

func (bitcaskNode *BitcaskNode) AsynchronousSync(req *node.LogEntryRequest) {
	for _, rpc := range bitcaskNode.slavesRpc {
		ctx, _ := context.WithTimeout(context.Background(), time.Second*1)
		rpc.OpLogEntry(ctx, req)
	}
}

func (bitcaskNode *BitcaskNode) SemiSynchronousSync(req *node.LogEntryRequest) {
}

func (bitcaskNode *BitcaskNode) SynchronousSync(req *node.LogEntryRequest) {
}
