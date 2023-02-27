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
		if bitcaskNode.slavesStatus[slaveId] != nodeInIdle {
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
		if bitcaskNode.slavesStatus[slaveId] != nodeInIdle {
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
		if bitcaskNode.slavesStatus[slaveId] != nodeInIdle {
			continue
		}
		ctx, _ := context.WithTimeout(context.Background(), config.RpcTimeOut)
		rpc.OpLogEntry(ctx, req)
	}
}

// 全量复制
func (bitcaskNode *BitcaskNode) FullReplication(slaveId string) {
	// TODO
	// 获取各索引对应的所有kv
	defer func() {
		if _, ok := bitcaskNode.slavesStatus[slaveId]; ok {
			bitcaskNode.slavesStatus[slaveId] = nodeInIdle
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

		req := packLogEntryRequest("set", value)
		bitcaskNode.syncChan <- syncChanItem{
			req:     req,
			slaveId: slaveId,
		}
	}

	// 发送所有list类型的数据
	keys, err = bitcaskNode.db.GetListKeys()
	if err != nil {
		log.Errorf("GetStrsKeys err [%v]", err)
		return
	}
	for _, key := range keys {
		values, err := bitcaskNode.db.LRange(key, 0, -1)
		// value, err := bitcaskNode.db.Get(key)
		if err != nil {
			log.Errorf("db.LRange err [%v]", err)
		}
		for _, value := range values {
			req := packLogEntryRequest("rpush", value)
			bitcaskNode.syncChan <- syncChanItem{
				req:     req,
				slaveId: slaveId,
			}
		}
	}
	// 发送所有hash类型的数据
	keys, err = bitcaskNode.db.HKeys()
	if err != nil {
		log.Errorf("GetStrsKeys err [%v]", err)
		return
	}
	for _, key := range keys {
		values, err := bitcaskNode.db.HGetAll(key)
		// value, err := bitcaskNode.db.Get(key)
		if err != nil {
			log.Errorf("db.LRange err [%v]", err)
		}
		for _, value := range values {
			// TODO packxxx函数可能要重构下,不同类型的cmd有点不同
			req := packLogEntryRequest("rpush", value)
			bitcaskNode.syncChan <- syncChanItem{
				req:     req,
				slaveId: slaveId,
			}
		}
	}

	// 根据kv构建对应的request

	// 发送写命令

	// 通知 全量复制完成/失败 客户端再决定要干嘛
}

func packLogEntryRequest(cmd string, value []byte) *node.LogEntryRequest {
	return &node.LogEntryRequest{
		Cmd:   string(cmd),
		Args_: []string{string(value)},
	}
}

// 发送写命令给指定的slave
func (bitcaskNode *BitcaskNode) SyncLogEntryToSlave() {
	for {
		select {
		case reqPack := <-bitcaskNode.syncChan:
			req := reqPack.req
			id := reqPack.slaveId
			if rpc, ok := bitcaskNode.slavesRpc[id]; ok {
				// TODO 发送失败,又该如何通知
				rpc.OpLogEntry(context.TODO(), req)
			}
		// case <- bitcaskNode.done
		default:
		}
	}
}

// 增量复制
func (bitcaskNode *BitcaskNode) IncreReplication(slaveId string, offset int64) {
	// 已有协程在进行增量复制
	if bitcaskNode.slavesStatus[slaveId] == nodeInIncrRepl {
		return
	}

	defer func() {
		log.Info(fmt.Sprintf("与slave[%s]增量复制完成", slaveId))
		bitcaskNode.slavesStatus[slaveId] = nodeInIdle
		// TODO 通知slave增量复制完成 改变状态

	}()

	slaveRpc := bitcaskNode.slavesRpc[slaveId]

	for i := 0; i < bitcaskNode.cf.CurReplicationOffset-int(offset); i++ {
		log.Infof("与slave[%s]进行增量复制.", slaveId)
		bitcaskNode.cacheMu.Lock()
		iReq, ok := bitcaskNode.opCache.Get(fmt.Sprintf("%d", offset+int64(i)))
		bitcaskNode.cacheMu.Unlock()

		if !ok {
			// 缓存查找失败,增量复制失败,通知slave复制终止
			slaveRpc.IncrReplFailNotify(context.Background(), bitcaskNode.cf.ID)
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
			slaveRpc.IncrReplFailNotify(context.Background(), bitcaskNode.cf.ID)
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
