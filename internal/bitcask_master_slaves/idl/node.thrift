namespace go user

# enum ErrCode {
#     SuccessCode                = 0
#     ServiceErrCode             = 10001
#     ParamErrCode               = 10002
#     UserAlreadyExistErrCode    = 10003
#     AuthorizationFailedErrCode = 10004
# }

struct BaseResp {
    1: i64 status_code
    2: string status_message
    3: i64 service_time
}

# 注册从节点
struct SlaveOfRequest {
    1: string address   // 
}

struct SlaveOfRespone {
    1: BaseResp base_resp
}

# 数据传输
enum OperationCode {
    Insert = 0
    Delete = 1
}

struct LogEntry {
    1: OperationCode opCode
    2: string key
    3: string value
    4: i64 score
    5: i64 expireAt
}

# 全量复制
struct FullReplicationReq {
    1: i64 logentry_id
}

struct FullReplicationResp {
    1: i64 logentry_id
    2: LogEntry entry
}
# 增量复制


# 健康检测
struct PingReq {
    1: bool ping
}

struct PingResp {
    1: bool status
}
# struct User {
#     1: i64 id
#     2: string username
#     3: i64 follow_count
#     4: i64 follower_count
#     # 5: string password
# }

# struct CreateUserRequest {
#     1: string username (vt.min_size = "1")
#     2: string password (vt.min_size = "1")
# }

# struct CreateUserResponse {
#     1: BaseResp base_resp
#     2: User user
# }

# struct MGetUserRequest {
#     1: list<i64> user_ids 
#     2: list<string> usernames 
# }

# struct MGetUserResponse {
#     1: list<User> users
#     2: BaseResp base_resp
# }

# struct CheckUserRequest {
#     1: string username 
#     2: string password 
# }

# struct CheckUserResponse {
#     1: i64 user_id
#     2: BaseResp base_resp
# }

# service UserService {
#     CreateUserResponse CreateUser(1: CreateUserRequest req)
#     MGetUserResponse MGetUser(1: MGetUserRequest req)
#     CheckUserResponse CheckUser(1: CheckUserRequest req)
# }