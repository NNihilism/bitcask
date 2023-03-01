namespace go prxyService


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

service ProxyService {
    # 供客户端调用
    bool Ping(1: bool req)

    LogEntryResponse OpLogEntry(1: LogEntryRequest req)
}
