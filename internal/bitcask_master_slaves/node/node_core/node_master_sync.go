package nodeCore

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node/nodeservice"
	"bitcaskDB/internal/log"
	"bitcaskDB/internal/util"
	"context"
	"fmt"
	"sync"
)

// 异步更新
func (bitcaskNode *BitcaskNode) AsynchronousSync(req *node.LogEntryRequest) {
	bitcaskNode.slavesRpc.Range(func(iSlaveId, iRpc interface{}) bool {
		slaveId, _ := iSlaveId.(string)
		rpc, _ := iRpc.(nodeservice.Client)

		// 子节点正在进行全量/增量复制
		if status, ok := bitcaskNode.getSlaveStatus(slaveId); !ok || status != nodeInIdle {
			return true
		}

		ctx, _ := context.WithTimeout(context.Background(), config.RpcTimeOut)
		rpc.OpLogEntry(ctx, req)
		return true
	})

}

// 半同步更新
func (bitcaskNode *BitcaskNode) SemiSynchronousSync(req *node.LogEntryRequest) {

	wg := new(sync.WaitGroup)
	semi_cnt := int(float64(bitcaskNode.cf.ConnectedSlaves) * config.SemiSynchronousRate)
	wg.Add(semi_cnt)

	bitcaskNode.slavesRpc.Range(func(iSlaveId, iRpc interface{}) bool {
		slaveId, _ := iSlaveId.(string)
		rpc, _ := iRpc.(nodeservice.Client)

		// 子节点正在进行全量/增量复制
		if status, ok := bitcaskNode.getSlaveStatus(slaveId); !ok || status != nodeInIdle {
			return true
		}

		go func(rpc nodeservice.Client) {
			defer wg.Done()
			ctx, _ := context.WithTimeout(context.Background(), config.RpcTimeOut)
			rpc.OpLogEntry(ctx, req)
		}(rpc)

		return true
	})

	wg.Wait()
}

// 同步更新
func (bitcaskNode *BitcaskNode) SynchronousSync(req *node.LogEntryRequest) {
	bitcaskNode.slavesRpc.Range(func(iSlaveId, iRpc interface{}) bool {
		slaveId, _ := iSlaveId.(string)
		rpc, _ := iRpc.(nodeservice.Client)

		// 子节点正在进行全量/增量复制
		if status, ok := bitcaskNode.getSlaveStatus(slaveId); !ok || status != nodeInIdle {
			return true
		}

		ctx, _ := context.WithTimeout(context.Background(), config.RpcTimeOut)
		rpc.OpLogEntry(ctx, req)
		return true
	})
}

// 全量复制
func (bitcaskNode *BitcaskNode) FullReplication(slaveId string) {
	defer func() {

		if _, ok := bitcaskNode.getSlaveStatus(slaveId); ok {
			bitcaskNode.slavesStatus.Store(slaveId, nodeInIdle)
		}
	}()

	// 发送所有string类型的数据
	keys, err := bitcaskNode.db.GetStrsKeys()
	if err != nil {
		log.Errorf("GetStrsKeys err [%v]", err)
		return
	}
	for _, key := range keys {
		value, err := bitcaskNode.db.Get(key)
		if err != nil {
			log.Errorf("db.Get err [%v]", err)
		}
		req := packLogEntryRequest("string", [][]byte{key, value})
		bitcaskNode.syncChan <- syncChanItem{
			req:     req,
			slaveId: slaveId,
		}
	}

	// 发送所有list类型的数据
	keys, err = bitcaskNode.db.GetListKeys()
	if err != nil {
		log.Errorf("GetListKeys err [%v]", err)
		return
	}
	for _, key := range keys {
		values, err := bitcaskNode.db.LRange(key, 0, -1)
		if err != nil {
			log.Errorf("db.LRange err [%v]", err)
		}
		req := packLogEntryRequest("list", append([][]byte{key}, values...))
		bitcaskNode.syncChan <- syncChanItem{
			req:     req,
			slaveId: slaveId,
		}
	}

	// 发送所有hash类型的数据
	keys, err = bitcaskNode.db.HKeys()
	if err != nil {
		log.Errorf("GetHashKeys err [%v]", err)
		return
	}
	for _, key := range keys {
		values, err := bitcaskNode.db.HGetAll(key)
		if err != nil {
			log.Errorf("db.LRange err [%v]", err)
		}
		req := packLogEntryRequest("hash", append([][]byte{key}, values...))
		bitcaskNode.syncChan <- syncChanItem{
			req:     req,
			slaveId: slaveId,
		}
	}

	// 发送所有set类型的数据
	keys, err = bitcaskNode.db.GetSetKeys()
	if err != nil {
		log.Errorf("GetSetKeys err [%v]", err)
		return
	}
	for _, key := range keys {
		values, err := bitcaskNode.db.SMembers(key)
		if err != nil {
			log.Errorf("db.LRange err [%v]", err)
		}
		req := packLogEntryRequest("set", append([][]byte{key}, values...))
		bitcaskNode.syncChan <- syncChanItem{
			req:     req,
			slaveId: slaveId,
		}
	}

	// 发送所有zset类型的数据
	keys = bitcaskNode.db.ZKeys()

	for _, key := range keys {
		members := bitcaskNode.db.ZMembers(key)
		for _, member := range members {
			ok, score := bitcaskNode.db.ZScore(key, member)
			if !ok {
				log.Errorf("unexist key:%s, member:%s", key, member)
				continue
			}
			req := packLogEntryRequest("zset", [][]byte{key, []byte(fmt.Sprintf("%f", score)), member})
			bitcaskNode.syncChan <- syncChanItem{
				req:     req,
				slaveId: slaveId,
			}
		}
	}

	// 通知 全量复制完成/失败 客户端再决定要干嘛

	rpc, ok := bitcaskNode.getSlaveRPC(slaveId)
	if !ok {
		return
	}
	ok, err = rpc.ReplFinishNotify(context.Background(), &node.ReplFinishNotifyReq{
		Ok:       true,
		SyncType: int8(config.FullReplSync),
	})
	// err或者不ok 都移除该异常节点
	if err != nil || !ok {
		bitcaskNode.RemoveSlave(slaveId)
	}

}

func packLogEntryRequest(datatype string, value [][]byte) *node.LogEntryRequest {
	var cmd string
	switch datatype {
	case "string":
		cmd = "set"
	case "list":
		cmd = "rpush"
	case "set":
		cmd = "sadd"
	case "hash":
		cmd = "hset"
	case "zset":
		cmd = "zadd"
	}
	args := util.BytesArrToStrArr(value)

	return &node.LogEntryRequest{
		Cmd:   cmd,
		Args_: args,
	}
}

// 发送写命令给指定的slave
func (bitcaskNode *BitcaskNode) SyncLogEntryToSlave(ctx context.Context) {
	for {
		select {
		case reqPack := <-bitcaskNode.syncChan:
			req := reqPack.req
			id := reqPack.slaveId

			if rpc, ok := bitcaskNode.getSlaveRPC(id); ok {
				rpc.OpLogEntry(context.TODO(), req)
			}

			// if rpc, ok := bitcaskNode.slavesRpc[id]; ok {
			// TODO 发送失败,又该如何通知
			// }
		case <-ctx.Done():
			return
		}
	}
}

// 增量复制
// offset : slave当前的offset
func (bitcaskNode *BitcaskNode) IncreReplication(slaveId string, offset int64) {
	// 已有协程在进行增量复制
	if status, ok := bitcaskNode.slavesStatus.Load(slaveId); !ok {
		log.Errorf("Get slave[%s] status failed", slaveId)
	} else if status == nodeInIncrRepl {
		return
	}

	slaveRpc, ok := bitcaskNode.getSlaveRPC(slaveId)

	if !ok {
		log.Errorf("Get slave[%s] rpc failed", slaveId)
		bitcaskNode.RemoveSlave(slaveId)
		return
	}

	defer func() {
		// 无论是正常终止还是异常终止,都算完成
		log.Info(fmt.Sprintf("与slave[%s]增量复制完成", slaveId))
		bitcaskNode.changeSlaveSyncStatus(slaveId, nodeInIdle)
	}()

	log.Infof("offset : %d, master_offset : %d", offset, bitcaskNode.cf.CurReplicationOffset)
	for i := 1; i <= bitcaskNode.cf.CurReplicationOffset-int(offset); i++ {
		log.Infof("与slave[%s]进行增量复制.", slaveId)
		bitcaskNode.cacheMu.Lock()
		iReq, ok := bitcaskNode.opCache.Get(fmt.Sprintf("%d", offset+int64(i)))
		bitcaskNode.cacheMu.Unlock()

		if !ok {
			// 缓存查找失败,增量复制失败,通知slave复制终止
			slaveRpc.ReplFinishNotify(context.Background(), &node.ReplFinishNotifyReq{
				Ok:       false,
				SyncType: int8(config.IncreReplSync),
			})
			return
		}
		iCacheItem, ok := iReq.(*cacheItem)
		if !ok {
			log.Errorf("convert iReq to cacheItem failed [%v]")
			return
		}
		req := iCacheItem.req
		log.Infof("发送数据[%v]进行增量复制\n", req)

		resp, err := slaveRpc.OpLogEntry(context.Background(), req)
		if err != nil {
			log.Errorf("IncreReplication err [%v]", err)
		}
		if resp.BaseResp.StatusCode != int64(node.ErrCode_SuccessCode) {
			// 同步失败 通知终止
			slaveRpc.ReplFinishNotify(context.Background(), &node.ReplFinishNotifyReq{
				Ok:       false,
				SyncType: int8(config.IncreReplSync),
			})
			return
		}
	}

	// 通知slave增量复制完成 改变状态
	slaveRpc.ReplFinishNotify(context.Background(), &node.ReplFinishNotifyReq{
		Ok:       true,
		SyncType: int8(config.IncreReplSync),
	})
}

// 主节点收到PSync后，判断进行增量复制还是全量复制，将判断结果返回，并异步执行复制
func (bitcaskNode *BitcaskNode) HandlePSyncReq(req *node.PSyncRequest) (*node.PSyncResponse, error) {
	slave_repl_offset := req.Offset

	resp := new(node.PSyncResponse)
	// 如果缓存中能找到slave节点想要的偏移量， 则增量复制
	if _, ok := bitcaskNode.opCache.Get(fmt.Sprintf("%d", slave_repl_offset+1)); ok {
		// go 增量复制
		go bitcaskNode.IncreReplication(req.SlaveId, slave_repl_offset)
		resp.Code = int8(config.IncreReplSync)
	} else {
		// go 全量复制
		go bitcaskNode.FullReplication(req.SlaveId)
		resp.Code = int8(config.FullReplSync)
	}
	return resp, nil
}

func (bitcaskNode *BitcaskNode) FlushOpReqBuffer() {
	// TODO 用于将缓冲区内容刷新
}
