package nodeCore

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/log"
	"context"
	"fmt"
	"sync"
)

// 异步更新
func (bitcaskNode *BitcaskNode) AsynchronousSync(req *node.LogEntryRequest) {
	for slaveId, rpc := range bitcaskNode.slavesRpc {
		// 子节点正在进行全量/增量复制
		if bitcaskNode.slavesStatus[slaveId] != slaveInIdle {
			continue
		}
		ctx, _ := context.WithTimeout(context.Background(), config.RpcTimeOut)
		rpc.OpLogEntry(ctx, req)
	}
}

// 半同步更新
func (bitcaskNode *BitcaskNode) SemiSynchronousSync(req *node.LogEntryRequest) {
	wg := new(sync.WaitGroup)
	semi_cnt := int(float64(len(bitcaskNode.slavesRpc)) * config.SemiSynchronousRate)
	wg.Add(semi_cnt)

	for slaveId, rpc := range bitcaskNode.slavesRpc {
		// 子节点正在进行全量/增量复制
		if bitcaskNode.slavesStatus[slaveId] != slaveInIdle {
			continue
		}
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
	for slaveId, rpc := range bitcaskNode.slavesRpc {
		// 子节点正在进行全量/增量复制
		if bitcaskNode.slavesStatus[slaveId] != slaveInIdle {
			continue
		}
		ctx, _ := context.WithTimeout(context.Background(), config.RpcTimeOut)
		rpc.OpLogEntry(ctx, req)
	}
}

// 全量复制
func (bitcaskNode *BitcaskNode) FullReplication(slaveId string) {

}

// 增量复制
func (bitcaskNode *BitcaskNode) IncreReplication(slaveId string, offset int64) {
	// 已有协程在进行增量复制
	if bitcaskNode.slavesStatus[slaveId] == slaveInIncrRepl {
		return
	}

	for i := 0; i < bitcaskNode.cf.CurReplicationOffset-int(offset); i++ {
		log.Infof("与slave[%d]进行增量复制.", slaveId)
		bitcaskNode.cacheMu.Lock()
		iReq, ok := bitcaskNode.opCache.Get(fmt.Sprintf("%d", offset+int64(i)))
		bitcaskNode.cacheMu.Unlock()

		if !ok {
			// 转全量复制，没能找到缓存
			// 状态设置
			return
		}
		iCacheItem, ok := iReq.(*cacheItem)
		req := iCacheItem.req
		log.Infof("发送数据req[%v]进行增量复制\n", req)

		resp, err := bitcaskNode.slavesRpc[slaveId].OpLogEntry(context.Background(), req)
		if err != nil {
			log.Errorf("IncreReplication err [%v]", err)
		}
		if resp.BaseResp.StatusCode != int64(node.ErrCode_SuccessCode) {
			// 同步失败 要不把节点删了？
			bitcaskNode.RemoveSlave(slaveId)
			return
		}
	}
}

// 主节点收到PSync后，判断进行增量复制还是全量复制，将判断结果返回，并异步执行复制
func (bitcaskNode *BitcaskNode) HandlePSyncReq(req *node.PSyncRequest) (*node.PSyncResponse, error) {
	slave_repl_offset := req.Offset

	resp := new(node.PSyncResponse)
	// 如果缓存中能找到slave节点想要的偏移量， 则增量复制
	if _, ok := bitcaskNode.opCache.Get(fmt.Sprintf("%d", slave_repl_offset)); ok {
		// go 增量复制
		go bitcaskNode.IncreReplication(req.SlaveId, req.Offset)
		resp.Code = int8(config.IncreReplSync)
	} else {
		// go 全量复制
		go bitcaskNode.FullReplication(req.SlaveId)
		resp.Code = int8(config.FullReplSync)
	}
	return resp, nil
}
