namespace go node

# enum ErrCode {
#     SuccessCode                = 0
#     ServiceErrCode             = 10001
#     ParamErrCode               = 10002
#     UserAlreadyExistErrCode    = 10003
#     AuthorizationFailedErrCode = 10004
# }

# struct BaseResp {
#     1: i64 status_code
#     2: string status_message
#     3: i64 service_time
# }

# 注册从节点
struct SlaveOfRequest {
    1: string address   // 从节点地址
    2: string runId  // 从节点的唯一标识
}

struct SlaveOfRespone {
    # 1: BaseResp base_resp
    1: string runId // 主节点的唯一标识
    2: i64 offset // 服务器的进度
}

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
    1: i64 master_id
    2: i64 slave_id
    3: i64 offset   // 从节点的复制进度，如果为-1则表示全量复制，否则为增量复制
}

struct PSyncResponse {
    1: i8 code
    # 2: LogEntry entry
}

# 数据操作，即数据的增删改查（被请求方将会被写入数据）
struct LogEntryRequest {
    1: i64 entry_id // 客户端发起的请求不会有这个标识，主节点发送请求时会有这个标识，方便进度同步
    2: OperationCode opCode
    3: LogEntry entry
}

struct LogEntryResponse {
    1: bool code
    2: LogEntry entry
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


service NodeService {
    SlaveOfRespone SlaveOf(1: SlaveOfRequest req)
    PSyncResponse PSync(1: PSyncRequest req)
    LogEntryRequest OpLogEntry(1: LogEntryRequest req)
    PingResponse Ping()
    InfoResponse Info()
}
