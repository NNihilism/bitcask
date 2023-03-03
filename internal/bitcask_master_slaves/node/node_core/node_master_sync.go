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
	bitcaskNode.slavesInfo.Range(func(id, iInfo interface{}) bool {
		info, ok := iInfo.(*slaveInfo)
		if !ok {
			log.Errorf("Convert iInfo[%v] to info failed", iInfo)
			return true
		}

		// 子节点正在进行全量/增量复制
		info.mu.Lock()
		if info.status != nodeInIdle {
			if info.status == nodeInFullRepl { // 全量复制才需要写入缓冲，增量复制不更新就行了，自有协程会补上
				bitcaskNode.writeToBuffer(info.id, req)
			}
			info.mu.Unlock()
			return true
		}
		info.mu.Unlock()

		rpc := info.rpc
		go func(rpc nodeservice.Client) {
			ctx, _ := context.WithTimeout(context.Background(), config.RpcTimeOut)
			rpc.OpLogEntry(ctx, req)
		}(rpc)

		return true
	})
}

// 半同步更新
func (bitcaskNode *BitcaskNode) SemiSynchronousSync(req *node.LogEntryRequest) {
	semi_cnt := int(float64(bitcaskNode.cf.ConnectedSlaves) * config.SemiSynchronousRate)
	ch := make(chan struct{})
	bitcaskNode.slavesInfo.Range(func(id, iInfo interface{}) bool {
		info, ok := iInfo.(*slaveInfo)
		if !ok {
			log.Errorf("Convert iInfo[%v] to info failed", iInfo)
			return true
		}
		// 子节点正在进行全量/增量复制
		info.mu.Lock()
		if info.status != nodeInIdle {
			if info.status == nodeInFullRepl { // 全量复制才需要写入缓冲，增量复制不更新就行了，自有协程会补上
				bitcaskNode.writeToBuffer(info.id, req)
			}
			info.mu.Unlock()
			return true
		}
		info.mu.Unlock()

		rpc := info.rpc
		go func(rpc nodeservice.Client) {
			ctx, _ := context.WithTimeout(context.Background(), config.RpcTimeOut)
			rpc.OpLogEntry(ctx, req)
			<-ch
		}(rpc)

		return true
	})

	for i := 0; i < semi_cnt; i++ {
		ch <- struct{}{}
	}
	close(ch)
}

// 同步更新
func (bitcaskNode *BitcaskNode) SynchronousSync(req *node.LogEntryRequest) {
	wg := new(sync.WaitGroup)
	bitcaskNode.slavesInfo.Range(func(id, iInfo interface{}) bool {
		info, ok := iInfo.(*slaveInfo)
		if !ok {
			log.Errorf("Convert iInfo[%v] to info failed", iInfo)
			return true
		}

		// 子节点正在进行全量/增量复制
		info.mu.Lock()
		if info.status != nodeInIdle {
			if info.status == nodeInFullRepl { // 全量复制才需要写入缓冲，增量复制不更新就行了，自有协程会补上
				bitcaskNode.writeToBuffer(info.id, req)
			}
			info.mu.Unlock()
			return true
		}
		info.mu.Unlock()

		rpc := info.rpc
		wg.Add(1)
		go func(rpc nodeservice.Client) {
			defer wg.Done()
			ctx, _ := context.WithTimeout(context.Background(), config.RpcTimeOut)
			rpc.OpLogEntry(ctx, req)
		}(rpc)

		return true
	})
	wg.Wait()
}

// 全量复制
func (bitcaskNode *BitcaskNode) FullReplication(slaveId string) {
	// 数据解析与数据发送可以并发执行
	wg := sync.WaitGroup{}
	wg.Add(1)
	syncChan := make(chan syncChanItem, config.SyncChanSize)
	go bitcaskNode.SyncLogEntryToSlave(&wg, syncChan)

	// 由于没有快照功能 因此下面这段扫描代码需要上一个”全局锁“
	bitcaskNode.mu.Lock()
	var tmp_entry_id int64 = 1                             // 供slave校验用,记录发送的记录个数是否正确
	curMasterOffset := bitcaskNode.cf.CurReplicationOffset // 记录下此时的offset
	// 发送所有string类型的数据
	keys, err := bitcaskNode.db.GetStrsKeys()
	if err != nil {
		log.Errorf("GetStrsKeys err [%v]", err)
		bitcaskNode.changeSlaveSyncStatus(slaveId, nodeInIdle)
		bitcaskNode.mu.Unlock()
		return
	}
	for _, key := range keys {
		value, err := bitcaskNode.db.Get(key)
		if err != nil {
			log.Errorf("db.Get err [%v]", err)
		}
		req := packLogEntryRequest("string", [][]byte{key, value}, bitcaskNode.cf.ID, tmp_entry_id)
		tmp_entry_id += 1
		syncChan <- syncChanItem{
			req:     req,
			slaveId: slaveId,
		}
	}

	// 发送所有list类型的数据
	keys, err = bitcaskNode.db.GetListKeys()
	if err != nil {
		log.Errorf("GetListKeys err [%v]", err)
		bitcaskNode.changeSlaveSyncStatus(slaveId, nodeInIdle)
		bitcaskNode.mu.Unlock()
		return
	}
	for _, key := range keys {
		values, err := bitcaskNode.db.LRange(key, 0, -1)
		if err != nil {
			log.Errorf("db.LRange err [%v]", err)
		}
		req := packLogEntryRequest("list", append([][]byte{key}, values...), bitcaskNode.cf.ID, tmp_entry_id)
		tmp_entry_id += 1
		syncChan <- syncChanItem{
			req:     req,
			slaveId: slaveId,
		}
	}

	// 发送所有hash类型的数据
	keys, err = bitcaskNode.db.HKeys()
	if err != nil {
		log.Errorf("GetHashKeys err [%v]", err)
		bitcaskNode.mu.Unlock()
		bitcaskNode.changeSlaveSyncStatus(slaveId, nodeInIdle)
		return
	}
	for _, key := range keys {
		values, err := bitcaskNode.db.HGetAll(key)
		if err != nil {
			log.Errorf("db.LRange err [%v]", err)
		}
		req := packLogEntryRequest("hash", append([][]byte{key}, values...), bitcaskNode.cf.ID, tmp_entry_id)
		tmp_entry_id += 1

		syncChan <- syncChanItem{
			req:     req,
			slaveId: slaveId,
		}
	}

	// 发送所有set类型的数据
	keys, err = bitcaskNode.db.GetSetKeys()
	if err != nil {
		log.Errorf("GetSetKeys err [%v]", err)
		bitcaskNode.mu.Unlock()
		bitcaskNode.changeSlaveSyncStatus(slaveId, nodeInIdle)
		return
	}
	for _, key := range keys {
		values, err := bitcaskNode.db.SMembers(key)
		if err != nil {
			log.Errorf("db.LRange err [%v]", err)
		}
		req := packLogEntryRequest("set", append([][]byte{key}, values...), bitcaskNode.cf.ID, tmp_entry_id)
		tmp_entry_id += 1
		syncChan <- syncChanItem{
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
			req := packLogEntryRequest("zset", [][]byte{key, []byte(fmt.Sprintf("%f", score)), member}, bitcaskNode.cf.ID, tmp_entry_id)
			tmp_entry_id += 1
			syncChan <- syncChanItem{
				req:     req,
				slaveId: slaveId,
			}
		}
	}
	log.Info("MASTER :  Data parsing completed.")
	bitcaskNode.mu.Unlock() // "快照部分结束"

	// 通知 全量复制完成/失败 客户端再决定要干嘛
	rpc, ok := bitcaskNode.getSlaveRPC(slaveId)
	if !ok {
		bitcaskNode.changeSlaveSyncStatus(slaveId, nodeInIdle)
		return
	}

	close(syncChan)
	wg.Wait() // 等待数据都发送完了再通知 不然slave可能先收到结束通知导致不接收尚未发送完的数据

	ok, err = rpc.ReplFinishNotify(context.Background(), &node.ReplFinishNotifyReq{
		Ok:           true,
		SyncType:     int8(config.FullReplSync),
		LastEntryId:  tmp_entry_id - 1, // 供slave校验用
		MasterOffset: int64(curMasterOffset),
	})

	// err或者不ok 都移除该异常节点
	if err != nil || !ok {
		log.Info("remove slave [%s], [err != nil : %v], [ok : %v]", slaveId, err != nil, ok)
		bitcaskNode.RemoveSlave(slaveId)
	}

	// ”快照“部分没问题，则刷新缓冲区
	bitcaskNode.flushAndUpdate(slaveId)
}

func packLogEntryRequest(datatype string, value [][]byte, masterId string, entryId int64) *node.LogEntryRequest {
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
		EntryId:  entryId,
		Cmd:      cmd,
		Args_:    args,
		MasterId: masterId,
	}
}

// 发送写命令给指定的slave
// 异步开启此方法,一边解析一边发送
func (bitcaskNode *BitcaskNode) SyncLogEntryToSlave(wg *sync.WaitGroup, syncChan chan syncChanItem) {
	defer wg.Done()

	for {
		select {
		case reqPack, ok := <-syncChan:
			if !ok {
				log.Info("MASTER :  Data transmission  completed.")
				return
			}
			// log.Info("读取reqPack. req : [%v], id : [%v]", reqPack.req, reqPack.slaveId)
			req := reqPack.req
			id := reqPack.slaveId

			if rpc, ok := bitcaskNode.getSlaveRPC(id); ok {
				log.Infof("发送 reqPack. req : [%v], id : [%v]", reqPack.req, reqPack.slaveId)
				rpc.OpLogEntry(context.TODO(), req)
			}

			// if rpc, ok := bitcaskNode.slavesRpc[id]; ok {
			// TODO 发送失败,又该如何通知
			// }
			// case <-ctx.Done():
			// 	return
		}
	}
}

// 增量复制
// offset : slave当前的offset
func (bitcaskNode *BitcaskNode) IncreReplication(slaveId string, offset int64) {
	// if status, ok := bitcaskNode.slavesStatus.Load(slaveId); !ok {
	// 	log.Errorf("Get slave[%s] status failed", slaveId)
	// } else if status == nodeInIncrRepl {
	// 	return
	// }

	// bitcaskNode.changeSlaveSyncStatus(slaveId, nodeInIncrRepl)

	log.Infof("here...")
	if ok := bitcaskNode.casSlaveSyncStatus(slaveId, nodeInIdle, nodeInIncrRepl); !ok {
		// 已有协程在进行增量复制
		return
	}
	log.Infof("Start incre repl for slave[%s]", slaveId)

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
		iReq, ok := bitcaskNode.replBakBuffer.Get(fmt.Sprintf("%d", offset+int64(i)))
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
		req.MasterId = bitcaskNode.cf.ID
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
	// 如果缓存中能找到slave节点想要的偏移量，则增量复制
	if _, ok := bitcaskNode.replBakBuffer.Get(fmt.Sprintf("%d", slave_repl_offset+1)); ok {
		// 避免重复创建协程进行数据同步
		if status, ok := bitcaskNode.getSlaveStatus(req.SlaveId); !ok || status == nodeInIncrRepl {
			resp.Code = int8(config.Fail)
			return resp, nil
		}
		log.Infof("Ready to incre repl for slave[%s]", req.SlaveId)
		go bitcaskNode.IncreReplication(req.SlaveId, slave_repl_offset)
		resp.Code = int8(config.IncreReplSync)
	} else {
		// 避免重复创建协程进行数据同步
		if ok := bitcaskNode.casSlaveSyncStatus(req.SlaveId, nodeInIdle, nodeInFullRepl); !ok {
			return resp, nil
		}

		//  感觉仍然有并发问题?比如两个协程都判断到此时非Full,然后都进入到changeSlave,并开启协程. 解决方法: CAS
		// if status, ok := bitcaskNode.getSlaveStatus(req.SlaveId); !ok || status == nodeInFullRepl {
		// 	resp.Code = int8(config.Fail)
		// }
		// bitcaskNode.changeSlaveSyncStatus(req.SlaveId, nodeInFullRepl)

		// 全量复制与增量复制不同,此时不着急go一个FullReplication(),需要等slave准备好再来
		resp.Code = int8(config.FullReplSync)
	}
	return resp, nil
}

// 只有请求全量复制的slave才会发送这个PSyncReady请求
func (bitcaskNode *BitcaskNode) HandlePSyncReady(req *node.PSyncRequest) (*node.PSyncResponse, error) {
	resp := &node.PSyncResponse{
		Code: int8(config.FullReplSync),
	}
	go bitcaskNode.FullReplication(req.SlaveId)
	return resp, nil
}

// 刷新缓冲区数据并改变slave状态
func (bitcaskNode *BitcaskNode) flushAndUpdate(slaveId string) {
	info, ok := bitcaskNode.getSlaveInfo(slaveId)
	if !ok {
		return
	}

	info.mu.Lock()
	defer info.mu.Unlock()
	bitcaskNode.flushOpReqBuffer(info)
	bitcaskNode.changeSlaveSyncStatus(slaveId, nodeInIdle)
}
func (bitcaskNode *BitcaskNode) flushOpReqBuffer(info *slaveInfo) {
	rpc := info.rpc
	for i := 0; i < len(info.buffer.buf); i++ {
		rpc.OpLogEntry(context.Background(), info.buffer.buf[i])
	}
}

func (bitcaskNode *BitcaskNode) writeToBuffer(slaveId string, req *node.LogEntryRequest) {
	info, ok := bitcaskNode.getSlaveInfo(slaveId)
	if !ok {
		return
	}

	// info.buffer.mu.Lock()
	// defer info.buffer.mu.Unlock()

	info.buffer.buf = append(info.buffer.buf, req)
}
