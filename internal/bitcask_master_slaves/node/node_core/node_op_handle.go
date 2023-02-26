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
	"hkeys":   hKeys,
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
		return &node.LogEntryResponse{
			BaseResp: &node.BaseResp{
				StatusCode:    int64(node.ErrCode_OpLogEntryErrCode),
				StatusMessage: errno.NewErr(errno.ErrCodeUnknownCMD, &errno.ErrInfo{Cmd: command}).Error(),
				ServiceTime:   time.Now().Unix(),
			},
		}, nil
	}

	// 非Master节点在从节点上进行写操作
	if req.EntryId == 0 && bitcaskNode.cf.Role == config.Slave && isWriteOperation(command) {
		return &node.LogEntryResponse{
			BaseResp: &node.BaseResp{
				StatusCode:    int64(node.ErrCode_OpLogEntryErrCode),
				StatusMessage: errno.NewErr(errno.ErrCodeWriteOnSlave, nil).Error(),
				ServiceTime:   time.Now().Unix(),
			},
		}, nil
	}

	rpcResp := &node.LogEntryResponse{}
	// fmt.Println("command:", command)
	// fmt.Println("args:", args)
	if res, err := cmdFunc(bitcaskNode, util.StrArrToByteArr(args)); err != nil {
		rpcResp.BaseResp = &node.BaseResp{
			StatusCode:    int64(node.ErrCode_OpLogEntryErrCode),
			StatusMessage: err.Error(),
			ServiceTime:   time.Now().Unix(),
		}
	} else {
		iResp, err := pack.BuildResp(pack.OpLogEntryResp, res)
		if err != nil {
			log.Errorf("BuildResp err [%v]", err)
		}
		rpcResp = iResp.(*node.LogEntryResponse)
	}

	return rpcResp, nil

	// 若操作合法，则进行相应的数据操作

	// 若为读操作请求，则返回结果，否则进行下面的操作

	// 若为主节点，则根据配置文件，将该数据进行异步/同步/半同步进行数据更新
	// 同时，需要将该记录进行保存，以供其他节点进行增量复制

	// 若为从节点，则判断EntryId是否为所需要的记录，如若不是，则向主节点发出增量复制请求；若是，则将记录写入，并更新offset字段
	// return nil, nil
}

var cmdWriteOperationMap = map[string]struct{}{
	"get": {},
}

func isWriteOperation(command string) bool {
	if _, ok := cmdWriteOperationMap[command]; ok {
		return true
	}
	return false
}
