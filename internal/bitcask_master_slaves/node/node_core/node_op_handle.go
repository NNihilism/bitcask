package nodeCore

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/node/util/pack"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	"bitcaskDB/internal/log"
	"bitcaskDB/internal/util"
	"fmt"
)

var resultOK = "OK"

type cmdHandler func(bitcaskNode *BitcaskNode, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	// string commands
	"set":      set,
	"get":      get,
	"mget":     mGet,
	"getrange": getRange,
	"getdel":   getDel,
	"setex":    setEX,
	"setnx":    setNX,
	"mset":     mSet,
	"msetnx":   mSetNX,
	"append":   appendStr,
	"decr":     decr,
	"decrby":   decrBy,
	"incr":     incr,
	"incrby":   incrBy,
	"strlen":   strLen,

	// list
	"lpush":  lPush,
	"lpushx": lPushX,
	"rpush":  rPush,
	"rpushx": rPushX,
	"lpop":   lPop,
	"rpop":   rPop,
	"lmove":  lMove,
	"llen":   lLen,
	"lindex": lIndex,
	"lset":   lSet,
	"lrange": lRange,

	// hash commands
	"hset":    hSet,
	"hsetnx":  hSetNX,
	"hget":    hGet,
	"hmget":   hmGet,
	"hdel":    hDel,
	"hexists": hExists,
	"hlen":    hLen,
	"hfields": hFields,
	"hvals":   hVals,
	"hgetall": hGetAll,
	"hstrlen": hStrLen,
	"hscan":   hScan,
	"hincrby": hIncrBy,

	// set commands
	"sadd":      sAdd,
	"spop":      sPop,
	"srem":      sRem,
	"sismember": sIsMember,
	"smembers":  sMembers,
	"scard":     sCard,
	"sdiff":     sDiff,
	"sunion":    sUnion,

	// zset commands
	"zadd":      zAdd,
	"zscore":    zScore,
	"zrem":      zRem,
	"zcard":     zCard,
	"zrange":    zRange,
	"zrevrange": zRevRange,
	"zrank":     zRank,
	"zrevrank":  zRevRank,

	// generic commands
	"type": keyType,
	"del":  del,

	"ping": ping,
	"quit": nil,

	// server management commands
	"info": info,
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

// 处理Logentry操作申请
func (bitcaskNode *BitcaskNode) HandleOpLogEntryRequest(req *node.LogEntryRequest) (resp *node.LogEntryResponse, err error) {
	command := req.Cmd
	_, ok := supportedCommands[command]

	// 无法解析的命令
	if !ok {
		return &node.LogEntryResponse{
			BaseResp: pack.BuildBaseResp(node.ErrCode_OpLogEntryErrCode, errno.NewErr(errno.ErrCodeUnknownCMD, &errno.ErrInfo{Cmd: command})),
		}, nil
	}

	// 非Master节点在从节点上进行写操作
	if req.MasterId == "" && bitcaskNode.cf.Role == config.Slave && !isReadOperation(command) {
		return &node.LogEntryResponse{
			BaseResp: pack.BuildBaseResp(node.ErrCode_OpLogEntryErrCode, errno.NewErr(errno.ErrCodeWriteOnSlave, nil)),
		}, nil
	}

	log.Infof("receive opReq : [%v]", req)

	// 从节点收到了Master发来的写操作
	if bitcaskNode.cf.Role == config.Slave && !isReadOperation(command) {
		// 全量复制时，slave对序列号没要求
		// 非全量复制时，slave需要检查序列号是否合法
		if bitcaskNode.syncStatus != nodeInFullRepl && req.EntryId != int64(bitcaskNode.cf.CurReplicationOffset)+1 {
			if bitcaskNode.cf.MasterReplicationOffset < int(req.EntryId) {
				bitcaskNode.cf.MasterReplicationOffset = int(req.EntryId)
			}
			// 收到乱序包 请求发送正确的数据包
			bitcaskNode.sendPSyncReq()
			return
		}
	}

	// 判断结束，可以执行
	return bitcaskNode.executeOpReq(req)
}

// 执行LogEntry操作
func (bitcaskNode *BitcaskNode) executeOpReq(req *node.LogEntryRequest) (*node.LogEntryResponse, error) {
	command, args := req.Cmd, req.Args_
	cmdFunc := supportedCommands[command]
	fmt.Println("command:", command)
	fmt.Println("args:", args)
	if isReadOperation(command) {
		bitcaskNode.mu.RLock()
	} else {
		bitcaskNode.mu.Lock()
	}
	rpcResp := &node.LogEntryResponse{}

	if res, err := cmdFunc(bitcaskNode, util.StrArrToByteArr(args)); err != nil {
		log.Infof("cmdFunc err [%v]", err)
		rpcResp.BaseResp = pack.BuildBaseResp(node.ErrCode_OpLogEntryErrCode, err)
		if isReadOperation(command) {
			bitcaskNode.mu.RUnlock()
		} else {
			bitcaskNode.mu.Unlock()
		}
		return rpcResp, nil
	} else {
		iResp, err := pack.BuildResp(pack.OpLogEntryResp, res)
		if err != nil {
			log.Errorf("BuildResp err [%v]", err)
		}
		rpcResp = iResp.(*node.LogEntryResponse)
	}

	if isReadOperation(command) {
		bitcaskNode.mu.RUnlock()
		return rpcResp, nil // 读请求不需要更新任何配置
	} else {
		bitcaskNode.mu.Unlock()
	}

	if bitcaskNode.cf.Role == config.Slave {
		bitcaskNode.cf.CurReplicationOffset = int(req.EntryId)
		bitcaskNode.cf.MasterReplicationOffset = int(req.EntryId)
	} else if bitcaskNode.cf.Role == config.Master {
		bitcaskNode.cf.CurReplicationOffset += 1
		bitcaskNode.cf.MasterReplicationOffset += 1
		bitcaskNode.saveMasterConfig()
		// 对于并发写请求，先执行的将会被赋上相对小的序列号，同时由于锁机制，也将会先被放入缓存或者进行传播
		req.EntryId = int64(bitcaskNode.cf.CurReplicationOffset)
		req.MasterId = bitcaskNode.cf.ID
		bitcaskNode.AddCache(req)
	}

	// 若拓扑结构为星型，则从节点不需要进行后续操作
	if bitcaskNode.cf.Role == config.Slave && config.NodeTopology == config.Star {
		return rpcResp, nil
	}

	// 写操作 同步给从节点
	bitcaskNode.opPropagate(req)

	return rpcResp, nil
}

func (bitcaskNode *BitcaskNode) opPropagate(req *node.LogEntryRequest) {
	// 根据配置文件，将该数据进行异步/半同步/同步进行数据更新
	switch config.Synchronous {
	case config.Asynchronous:
		go bitcaskNode.AsynchronousSync(req)
	case config.SemiSynchronous:
		bitcaskNode.SemiSynchronousSync(req)
	case config.Synchronous:
		bitcaskNode.SynchronousSync(req)
	default:
		bitcaskNode.SynchronousSync(req)
	}
}

func isReadOperation(command string) bool {
	if _, ok := cmdReadOperationMap[command]; ok {
		return true
	}
	return false
}
