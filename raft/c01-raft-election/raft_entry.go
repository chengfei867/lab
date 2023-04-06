package raft

/**
raft日志复制
*/

// LogEntry 日志结构
type LogEntry struct {
	Term    int         //任期号
	Command interface{} //客户端命令
}
