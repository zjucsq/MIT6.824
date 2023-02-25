package kvraft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSEKV")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dError       logTopic = "ERROR"
	dPersist     logTopic = "DPERS"
	dWarn        logTopic = "DWARN"
	dKVGet       logTopic = "KVGET"
	dKVPut       logTopic = "KVPUT"
	dKVAppend    logTopic = "KVAPP"
	dKVPutAppend logTopic = "KVPPP"
	dCLGet       logTopic = "CLGET"
	dCLPut       logTopic = "CLPUT"
	dCLAppend    logTopic = "CLAPP"
	dCLPutAppend logTopic = "CLPPP"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func DebugReceiveGet(kv *KVServer, args *GetArgs) {
	Debug(dKVGet, "S%d Receive Get Request. Arg: %v", kv.me, args)
}

func DebugReplyGet(kv *KVServer, args *GetArgs, reply *GetReply) {
	Debug(dKVGet, "S%d Reply Get Request. Args: %v, Reply: %v", kv.me, args, reply)
}

func DebugReceivePutAppend(kv *KVServer, args *PutAppendArgs) {
	if args.Op == PUT {
		Debug(dKVPut, "S%d Receive Put Request. Key: %s, Val: %s", kv.me, args.Key, args.Value)
	} else if args.Op == APPEND {
		Debug(dKVAppend, "S%d Receive APP Request. Key: %s, Val: %s", kv.me, args.Key, args.Value)
	}
}

func DebugReplyPutAppend(kv *KVServer, args *PutAppendArgs, reply *PutAppendReply) {
	if args.Op == PUT {
		Debug(dKVPut, "S%d Reply Put Request. Key: %s, Val: %s, Reply: %v", kv.me, args.Key, args.Value, reply)
	} else if args.Op == APPEND {
		Debug(dKVAppend, "S%d Reply APP Request. Key: %s, Val: %s, Reply: %v", kv.me, args.Key, args.Value, reply)
	}
}
