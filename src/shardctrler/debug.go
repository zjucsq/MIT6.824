package shardctrler

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSECTRLER")
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
	dLogs    logTopic = "CRTDLOGS"
	dError   logTopic = "CRTERROR"
	dPersist logTopic = "CRTDPERS"
	dSnap    logTopic = "CRTDSNAP"
	dWarn    logTopic = "CRTDWARN"
	dSCJoin  logTopic = "CRTSCJOI"
	dSCMove  logTopic = "CRTSCMOV"
	dSCLeave logTopic = "CRTSCLEA"
	dSCQuery logTopic = "CRTSCQUE"
	// dCLGet       logTopic = "CLGET"
	// dCLPut       logTopic = "CLPUT"
	// dCLAppend    logTopic = "CLAPP"
	// dCLPutAppend logTopic = "CLPPP"
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
