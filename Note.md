# lab2

## lab2A

### About Mutex

A raft node has the following threads:
- The main thread
- voteTicker
- appendTicker
- When send a rpc call, create a thread
- When receive a rpc call, create a thread

A straightforward mutex strategy is locking all these threads when they are visiting shared variables.

One pitfall: The rpc call is blocking, so the caller cannot hold the mutex when sending rpc call, otherwise it will deadlock. 
(Suppose two caller send a rpc call to each other holding locks at the same time)

### About Expire Time

Raft算法中的三种超时时间 https://zhuanlan.zhihu.com/p/382348980

## lab2B

### About test functions

```go
// 在rf上尝试提交command，最后一个参数表示这个server是不是leader
// 如果最后一个参数为true，那么前两个参数分别表示该command如果成功，index和term分别是多少
func (rf *Raft) Start(command interface{}) (int, int, bool) {}

// 提交command直到成功，测试的时候保证一半以上节点存活时才会调用这个函数，所以不能达成一致就是出错
func (cfg *config) one(cmd interface{}, expectedServers int, retry bool) int {}
```

### About appendTicker

- Call AppendEntries every APPEND_SEND_TIME.
- Call AppendEntries when new data enters, only send HeartBeatPacket when timeout.
  - The tester limits you to 10 heartbeats per second.
  - We cannot know when the rpc call finished, so we may send multiple calls.
    - But in the first solution, we should also deal with the problem (Less frequent).
  - We should maintain a timer for each follow.

### About appendEntries

- Check args.Term
  - args.Term < rf.currentTerm: outdated, return false
  - args.Term > rf.currentTerm: update currentTerm
  - args.Term == rf.currentTerm: do nothing
- Check args.PrevLogIndex and args.PrevTermIndex
  - args.PrevLogIndex > rf.GetLastIndex() || rf.log/[args.PrevLogIndex/].Term != args.PrevTermIndex: return

## Lab2C

- For duplicated rpc call, we should check rf.nextIndex[idx] == prevLogIndex + 1. If not true, the response is out-of-date, throw it.
- Leader can ony commit logs in its term.
  - no-op log.

### About Go

https://blog.csdn.net/qq_45795744/article/details/125929752
slice can be seen as pointer, so remember to deep copy when use slice as the argument.

## Lab2D

some problem about apply, apply snapshot and logs may conflict.

# lab3

- Duplicate request
- Timeout

# lab4

## 4B

### trick about lock

Sometimes we cannot use defer lock, such as in a for loop. In this situation, we can use a function, and use defer in the function.