package shardkv

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSESHARDKV")
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
	dError       logTopic = "SKVERROR"
	dPersist     logTopic = "SKVDPERS"
	dSnap        logTopic = "SKVDSNAP"
	dWarn        logTopic = "SKVDWARN"
	dKVGet       logTopic = "SKVKVGET"
	dKVPut       logTopic = "SKVKVPUT"
	dKVAppend    logTopic = "SKVKVAPP"
	dKVPutAppend logTopic = "SKVKVPPP"
	dKVConfig    logTopic = "SKVKVCOF"
	dCLGet       logTopic = "SKVCLGET"
	dCLPut       logTopic = "SKVCLPUT"
	dCLAppend    logTopic = "SKVCLAPP"
	dCLPutAppend logTopic = "SKVCLPPP"
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
