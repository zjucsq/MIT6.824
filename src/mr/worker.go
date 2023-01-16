package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := AcJobArgs{}
		reply := AcJobReply{}
		ok := call("Coordinator.AcquireJob", &args, &reply)
		if !ok {
			// assume all errors are caused by the exit of the coordinator, so we exit
			log.Println("Call AcquireJob failed! Work exist!")
			break
		}
		job := reply.AcJob
		if job.JobType == WaitJob {
			time.Sleep(1 * time.Second)
		} else if job.JobType == MapJob {
			filename := job.File
			log.Println("Get map job, filename =", filename)
			file, err := os.Open(filename)

			if err != nil {
				log.Println("1")
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Println("2")
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			eachKva := make([][]KeyValue, reply.ReduceNum)
			for i := 0; i < reply.ReduceNum; i++ {
				eachKva[i] = []KeyValue{}
			}
			for _, kv := range kva {
				idx := ihash(kv.Key) % reply.ReduceNum
				eachKva[idx] = append(eachKva[idx], kv)
			}
			for i := 0; i < reply.ReduceNum; i++ {
				outFileName := fmt.Sprintf("mr-%d-%d", job.JobId, i)
				outFile, err := os.CreateTemp("./", outFileName)
				if err != nil {
					// log.Println("open tempfile", outFile.Name())
					log.Fatal(err)
				}

				enc := json.NewEncoder(outFile)
				for _, kv := range eachKva[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatal(err)
					}
				}
				outFile.Close()
				os.Rename(outFile.Name(), outFileName)
				// log.Println("Rename", outFile.Name(), "to", outFileName)
			}

			go func(job Job) {
				args := JobFinishArgs{job}
				reply := JobFinishReply{}
				ok := call("Coordinator.JobFinish", &args, &reply)
				if !ok {
					log.Fatal("map call taskdone error!")
				}
			}(job)

		} else if job.JobType == ReduceJob {
			log.Println("Get reduce job, job id = ", job.JobId)
			allReduceFile := make([]string, reply.MapNum)
			for i := 0; i < reply.MapNum; i++ {
				allReduceFile[i] = fmt.Sprintf("mr-%d-%d", i, job.JobId)
			}
			var kva []KeyValue
			for i := 0; i < reply.MapNum; i++ {
				infile, err := os.Open(allReduceFile[i])
				if err != nil {
					log.Fatal(err)
				}

				dec := json.NewDecoder(infile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				infile.Close()
			}

			sort.Sort(ByKey(kva))

			oname := fmt.Sprintf("mr-out-%d", job.JobId)
			ofile, _ := os.CreateTemp("./", oname)

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			ofile.Close()
			os.Rename(ofile.Name(), oname)

			go func(job Job) {
				args := JobFinishArgs{job}
				reply := JobFinishReply{}
				ok := call("Coordinator.JobFinish", &args, &reply)
				if !ok {
					log.Fatal("reduce call taskdone error!")
				}
			}(job)

		} else {
			log.Fatalln("Unknown job type.")
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//// example function to show how to make an RPC call to the coordinator.
////
//// the RPC argument and reply types are defined in rpc.go.
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}
//
//// send an RPC request to the coordinator, wait for the response.
//// usually returns true.
//// returns false if something goes wrong.
//func call(rpcname string, args interface{}, reply interface{}) bool {
//	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
//	sockname := coordinatorSock()
//	c, err := rpc.DialHTTP("unix", sockname)
//	if err != nil {
//		log.Fatal("dialing:", err)
//	}
//	defer c.Close()
//
//	err = c.Call(rpcname, args, reply)
//	if err == nil {
//		return true
//	}
//
//	fmt.Println(err)
//	return false
//}
