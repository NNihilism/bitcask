namespace go node

enum ErrCode {
    SuccessCode       = 0
    ServiceErrCode    = 10001
    ParamErrCode      = 10002
    SlaveofErrCode    = 10003
    OpLogEntryErrCode = 10004
}

struct BaseResp {
    1: i64 status_code
    2: string status_message
    3: i64 service_time
}

# 向master发送注册请求
struct RegisterSlaveRequest {
    1: string address   // 从节点地址
    2: string runId  // 从节点的唯一标识
}

struct RegisterSlaveResponse {
    1: BaseResp base_resp
    2: string runId // 主节点的唯一标识
    3: i64 offset // 服务器的进度
}

# client要求所连node发送RegisterSlave请求
struct SendSlaveofRequest {
    1: string address   // 目标Master地址
}

struct SendSlaveofResponse {
    1: BaseResp base_resp
}
# struct SlaveOfRequest {
#     1: string address   // 从节点地址
#     2: string runId  // 从节点的唯一标识
# }

# struct SlaveOfRespone {
#     1: BaseResp base_resp
#     2: string runId // 主节点的唯一标识
#     3: i64 offset // 服务器的进度
# }

# 数据传输
enum OperationCode {
    Insert = 0
    Delete = 1
    Query = 2
}

struct LogEntry {
    1: string key
    2: string value
    3: i64 score
    4: i64 expireAt
}

# 数据更新请求（从节点主动发出）
struct PSyncRequest {
    1: string master_id
    2: string slave_id
    3: i64 offset   // 从节点的复制进度，如果为-1则表示全量复制，否则为增量复制，若master判断无法满足增量复制条件，则开始进行全量复制
}

struct PSyncResponse {
    1: i8 code  //从节点根据状态码，判断接下来应该增量复制还是全量复制，并设置offset、syncstatus等字段信息
    # 2: LogEntry entry
}

# 数据操作，即数据的增删改查（被请求方将会被写入数据）
struct LogEntryRequest {
    1: i64 entry_id // 客户端发起的请求不会有这个标识，主节点发送请求时会有这个标识，方便进度同步
    # 2: OperationCode opCode
    # 3: LogEntry entry
    # TODO k/v带空格怎么办? 在解析args时不对""环绕内容进行split?
    2: string cmd  
    3: list<string> args
    4: string master_id
}

struct LogEntryResponse {
    1: BaseResp base_resp
    2: list<LogEntry> entries
    3: string info // 若状态码正确且entries为空，则输出info信息
}

# 健康检测
struct PingRequest {
    1: bool ping
}

struct PingResponse {
    1: bool status
}

# 节点信息打印
struct InfoRequest {
    1: bool ping
}

struct InfoResponse {
    1: string role
    2: i64 connected_slaves
    3: i64 master_replication_offset
    4: i64 cur_replication_offset
}

struct ReplFinishNotifyReq {
    1: i8 sync_type
    2: bool ok
    3: i64 master_offset
}

service NodeService {
    # for client
    SendSlaveofResponse SendSlaveof(1: SendSlaveofRequest req)
    # for other node
    RegisterSlaveResponse RegisterSlave(1: RegisterSlaveRequest req)

    bool ReplFinishNotify(ReplFinishNotifyReq req)
    # bool IncrReplFailNotify(string masterId)    // 增量复制失败时,master用于通知slave增量复制终止
    PSyncResponse PSync(1: PSyncRequest req)
    LogEntryResponse OpLogEntry(1: LogEntryRequest req)
    PingResponse Ping()
    InfoResponse Info()
}
