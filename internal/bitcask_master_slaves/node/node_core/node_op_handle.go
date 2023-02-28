package nodeCore

import (
	"bitcaskDB/internal/bitcask_master_slaves/node/config"
	"bitcaskDB/internal/bitcask_master_slaves/node/kitex_gen/node"
	"bitcaskDB/internal/bitcask_master_slaves/node/util/pack"
	"bitcaskDB/internal/bitcask_master_slaves/pkg/errno"
	"bitcaskDB/internal/log"
	"bitcaskDB/internal/util"
	"time"
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

func (bitcaskNode *BitcaskNode) HandleOpLogEntryRequest(req *node.LogEntryRequest) (resp *node.LogEntryResponse, err error) {
	// type LogEntryRequest struct {
	// 	EntryId int64    `thrift:"entry_id,1" frugal:"1,default,i64" json:"entry_id"`
	// 	Cmd     string   `thrift:"cmd,2" frugal:"2,default,string" json:"cmd"`
	// 	Args_   []string `thrift:"args,3" frugal:"3,default,list<string>" json:"args"`
	// }

	// type LogEntryResponse struct {
	// 	Code  bool        `thrift:"code,1" frugal:"1,default,bool" json:"code"`
	// 	Entry []*LogEntry `thrift:"entry,2" frugal:"2,default,list<LogEntry>" json:"entry"`
	// }

	// 若role == slave，则只能接受Master的写命令进行数据同步，其他节点/客户端发送的写命令不执行
	// EntryId > 0，则为Master发送的请求
	command, args := req.Cmd, req.Args_
	cmdFunc, ok := supportedCommands[command]

	// 无法解析的命令
	if !ok {
		// TODO PackResponse的包装
		return &node.LogEntryResponse{
			BaseResp: &node.BaseResp{
				StatusCode:    int64(node.ErrCode_OpLogEntryErrCode),
				StatusMessage: errno.NewErr(errno.ErrCodeUnknownCMD, &errno.ErrInfo{Cmd: command}).Error(),
				ServiceTime:   time.Now().Unix(),
			},
		}, nil
	}

	// 非Master节点在从节点上进行写操作
	if req.EntryId == 0 && bitcaskNode.cf.Role == config.Slave && !isReadOperation(command) {
		return &node.LogEntryResponse{
			BaseResp: &node.BaseResp{
				StatusCode:    int64(node.ErrCode_OpLogEntryErrCode),
				StatusMessage: errno.NewErr(errno.ErrCodeWriteOnSlave, nil).Error(),
				ServiceTime:   time.Now().Unix(),
			},
		}, nil
	}

	// 从节点收到了Master发来的写操作
	if bitcaskNode.cf.Role == config.Slave && !isReadOperation(command) {
		// 如果处于syncbusy状态，表明正在进行全量复制，此时无视序列号，直接写入
		// 如果不是syncbush状态，则可能正在进行增量复制或者状态正常，则需要判断序列号，不合法则返回
		if bitcaskNode.synctatus != nodeInFullRepl && req.EntryId != int64(bitcaskNode.cf.CurReplicationOffset)+1 {
			// 更新已知最大进度
			if bitcaskNode.cf.MasterReplicationOffset < int(req.EntryId) {
				bitcaskNode.cf.MasterReplicationOffset = int(req.EntryId)
			}
			// 发送数据同步请求
			bitcaskNode.sendPSyncReq()
			return
		}
	}

	// 执行对应的操作
	rpcResp := &node.LogEntryResponse{}
	if res, err := cmdFunc(bitcaskNode, util.StrArrToByteArr(args)); err != nil {
		rpcResp.BaseResp = &node.BaseResp{
			StatusCode:    int64(node.ErrCode_OpLogEntryErrCode),
			StatusMessage: err.Error(),
			ServiceTime:   time.Now().Unix(),
		}
		// 无论主从节点，执行错误则不需要再继续后续操作
		return rpcResp, nil
	} else {
		iResp, err := pack.BuildResp(pack.OpLogEntryResp, res)
		if err != nil {
			log.Errorf("BuildResp err [%v]", err)
		}
		rpcResp = iResp.(*node.LogEntryResponse)
	}

	if bitcaskNode.cf.Role == config.Slave {
		// 进度相同，不会触发更新请求
		bitcaskNode.cf.CurReplicationOffset = int(req.EntryId)
	} else if bitcaskNode.cf.Role == config.Master {
		// 将该记录进行保存，以供其他节点进行增量复制
		// TODO 将offset写入数据库,但不将opCache写入,写入offset只是为了能让logentry与offset(logentryId)一一对应
		bitcaskNode.cf.CurReplicationOffset += 1
		bitcaskNode.cf.MasterReplicationOffset += 1
		req.EntryId = int64(bitcaskNode.cf.CurReplicationOffset)
		bitcaskNode.AddCache(req)
	}

	// 若拓扑结构为星型，则从节点不需要进行后续操作
	if bitcaskNode.cf.Role == config.Slave && config.NodeTopology == config.Star {
		return rpcResp, nil
	}

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

	return rpcResp, nil
}

func isReadOperation(command string) bool {
	if _, ok := cmdReadOperationMap[command]; ok {
		return true
	}
	return false
}
