package raft

/**
raft日志复制
*/

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// LogEntry 日志结构
type LogEntry struct {
	Term    int         //任期号
	Command interface{} //客户端命令
}

// AppendEntriesArgs  日志的请求结构
type AppendEntriesArgs struct {
	Term int //leader任期号
	//在raft中，有可能会出现直接连上follower的情况，此时，需要告诉client重定向给leader
	LeaderId     int        //leader
	PrevLogIndex int        //新的日志条目紧随之前的索引值
	PrevLogTerm  int        //prevLog的任期
	Entries      []LogEntry //准备存储的日志条目
	LeaderCommit int        //leader已经提交的日志索引
}

// AppendEntriesReply 日志响应结构
type AppendEntriesReply struct {
	CurrentTerm  int  //当前任期号，主要用于更新自己
	Success      bool //follower包含了匹配的prevLogIndex和preLogTerm的日志时为真
	ConflictTerm int  //冲突日志的任期编号
	FirstIndex   int  //存储第一个冲突编号的日志索引
}

// 应用日志进程
func (rf *Raft) applyEntryDaemon() {
	//日志提交完成之后
	//将日志进行应用、返回
	for {
		var logs []LogEntry
		rf.mu.Lock()
		//判断，如果节点最后的日志索引和已提交的日志索引相等
		//说明提交过的所有日志都已经被应用
		for rf.lastApplied == rf.commitIndex {
			rf.commitCond.Wait()
			select {
			//若发生终端
			case <-rf.shutdown:
				rf.mu.Unlock()
				close(rf.applyCh)
				return
			default:
			}
		}
		//获取最后应用的日志索引与最新提交的日志索引
		last, cur := rf.lastApplied, rf.commitIndex
		//说明当前存在已提交但未应用到状态机的日志
		if last < cur {
			rf.lastApplied = rf.commitIndex
			//截取已提交但未应用到状态机的这部分日志
			logs := make([]LogEntry, cur-last)
			copy(logs, rf.Logs[last+1:cur])
		}
		rf.mu.Unlock()
		//对还没有被应用的日志进行应用
		for i := 0; i < cur-last; i++ {
			reply := ApplyMsg{
				Index:   last + i + 1,
				Command: logs[i].Command,
			}
			//传回响应
			rf.applyCh <- reply
		}
	}
}

// 唤醒一致性检查
func (rf *Raft) wakeupConsistencyCheck() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.newEntryCond[i].Broadcast()
		}
	}
}

// 启动日志复制进程
func (rf *Raft) logEntryAgreeDaemon() {
	//遍历节点 向其他每个节点发起日志复制操作
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.consistencyCheckDaemon(i)
		}
	}
}

// 发起日志复制操作
func (rf *Raft) consistencyCheckDaemon(i int) {
	for {
		rf.mu.Lock()
		//每个节点都在等待client提交命令到leader上去
		rf.newEntryCond[i].Wait()
		select {
		case <-rf.shutdown:
			rf.mu.Unlock()
			return
		default:
		}

		//判断节点角色，只有leader才能发起日志复制
		if rf.isLeader {
			var args AppendEntriesArgs
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term
			//判断是否有新的日志进来
			//len(rf.Logs)表示当前leader当前日志总数
			//rf.nextIndex[i]发送给节点i的下一个日志索引
			//leader的日志长度大于leader所知道的follow i的日志长度
			if rf.nextIndex[i] < len(rf.Logs) {
				//添加新的日志
				args.Entries = append(args.Entries, rf.Logs[rf.nextIndex[i]:]...)
			} else {
				args.Entries = nil
			}
			rf.mu.Unlock()
			replyCh := make(chan AppendEntriesReply, 1)
			go func() {
				var reply AppendEntriesReply
				//发起日志复制请求
				if rf.sendAppendEntries(i, &args, &reply) {
					replyCh <- reply
				}
			}()

			//获取响应
			select {
			case reply := <-replyCh:
				rf.mu.Lock()
				if reply.Success {
					//说明响应成功
					rf.matchIndex[i] = len(rf.Logs) - 1
					rf.nextIndex[i] = len(rf.Logs)
					//提交日志(更新已提交的日志索引)
					rf.updateCommitIndex()
				} else {
					//响应失败
					//判断响应传回来的term与当前节点(leader)谁大
					//若当前节点的term更大
					if reply.CurrentTerm > args.Term {
						rf.VotedFor = -1
						//将自己的任期号改为当前reply的任期号
						rf.CurrentTerm = reply.CurrentTerm
					}
					//不能再担任leader,转变为follower
					if rf.isLeader {
						rf.isLeader = false
						//一致性检查
						rf.wakeupConsistencyCheck()
					}
					rf.mu.Unlock()
					rf.resetTimer <- struct{}{}
					return
				}
				//解决日志冲突
				//know:当前leader能否找到冲突
				//lastIndex代表当前节点中最后一个包含冲突任期号的日志索引
				var know, lastIndex = false, 0
				if reply.ConflictTerm != 0 {
					//找到最后的产生冲突的任期编号
					for i := len(rf.Logs) - 1; i > 0; i-- {
						//找到冲突编号
						if rf.Logs[i].Term == reply.ConflictTerm {
							know = true
							lastIndex = i
							break
						}
					}
					//如果找到冲突编号
					if know {
						//判断当前获取的冲突编号索引与想应中的冲突索引的大小
						if lastIndex > reply.FirstIndex {
							//说明在最后一个产生冲突的日志之前已经有一个冲突，我们只需要保存索引值最小的那个冲突的索引
							lastIndex = reply.FirstIndex
						}
						rf.nextIndex[i] = lastIndex
					} else {
						rf.nextIndex[i] = reply.FirstIndex
					}
				} else {
					//响应中没有冲突的日志编号
					rf.nextIndex[i] = reply.FirstIndex
				}
				//1 <=rf.nextIndex[i]<=len(rf.Logs)
				rf.nextIndex[i] = min(max(1, rf.nextIndex[i]), len(rf.Logs))
			}
			rf.mu.Unlock()
		} else {
			//当前节点不是leader直接返回
			rf.mu.Unlock()
			return
		}
	}
}

// 发起日志复制的请求
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 更新日志提交索引
func (rf *Raft) updateCommitIndex() {

}

// AppendEntries 接收到日志复制请求之后的处理
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	select {
	case <-rf.shutdown:
		return
	default:
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//判断任期,将比leader更新的任期填充到响应中
	if args.Term < rf.CurrentTerm {
		reply.CurrentTerm = rf.CurrentTerm
		reply.Success = false
		return
	}
	//如果当前角色还是leader则更改节点角色（强一致性）
	if rf.isLeader {
		rf.isLeader = false
		rf.wakeupConsistencyCheck()
	}
	//如果不相等，那么在出现client直接访问当前follower的时候
	//follower会重定向到一个错误的节点
	if rf.VotedFor != args.LeaderId {
		rf.VotedFor = args.LeaderId
	}
	//当前节点任期编号小于leader，则覆盖
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
	}
	//重置
	rf.resetTimer <- struct{}{}
	preLogIdx, preLogTerm := 0, 0
	//大于说明在follower的日志中能够找到args包含的prevLogIndex
	if len(rf.Logs) > args.PrevLogIndex {
		preLogIdx = args.PrevLogIndex
		//从follower节点的日志中获取到的与args传入的索引相同的任期编号
		preLogTerm = rf.Logs[preLogIdx].Term
	}

	//判断是否匹配
	if preLogIdx == args.PrevLogIndex && preLogTerm == args.PrevLogTerm {
		reply.Success = true
		//截取当前已知的最后一个匹配将不匹配的丢掉
		rf.Logs = rf.Logs[:preLogIdx+1]
		//追加日志
		rf.Logs = append(rf.Logs, args.Entries...)
		//获取更新之后的最后一个日志索引
		last := len(rf.Logs) - 1
		//更新commitIndex
		//如果leaderCommit > commitIndex,令commitIndex等于leaderCommit和新日志条目索引值中较小的一个
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, last)
			go func() { rf.commitCond.Broadcast() }()
		}
		//更新最后一个冲突日志
		reply.ConflictTerm = rf.Logs[last].Term
		reply.FirstIndex = last
	} else {
		reply.Success = false
		//处理冲突任期编号
		var first = 1
		reply.ConflictTerm = preLogTerm
		if reply.ConflictTerm == 0 {
			first = len(rf.Logs)
			//将响应的冲突条目设置为当前节点最后一个日志索引的条目
			reply.ConflictTerm = rf.Logs[first-1].Term
		} else {
			//说明任期编号有冲突 preLogTerm!=args.PrevLogTerm
			for i := preLogIdx - 1; i > 0; i-- {
				if rf.Logs[i].Term != preLogTerm {
					first = i + 1
					break
				}
			}
		}
		//第一个产生冲突的日志
		reply.FirstIndex = first
	}
}
