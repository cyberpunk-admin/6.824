package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	if err := CallMapTask(mapf); err != nil {
		fmt.Println(err)
		return
	}
	if err := CallReduceTask(reducef); err != nil {
		fmt.Println(err)
		return
	}
}

func readContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallMapTask(mapf func(string, string) []KeyValue) error {
	for {
		args := MapArg{}
		reply := MapReply{}
		call("Coordinator.AskForMapTask", &args, &reply)
		if reply.ID == -2 {
			break
		}
		if reply.ID == -1 {
			time.Sleep(2 * time.Second)
			continue
		}
		fmt.Println("[Map] now read file ", reply.Filename)
		contents := readContent(reply.Filename)
		kvs := mapf(reply.Filename, contents)
		tempFiles := map[int]*os.File{}
		for _, kv := range kvs {
			rId := ihash(kv.Key) % reply.NReduce
			var tf *os.File
			var err error
			if _, ok := tempFiles[rId]; !ok {
				tf, err = ioutil.TempFile("./", "mr-temp-*")
				tempFiles[rId] = tf
				if err != nil {
					panic(err)
				}
			} else {
				tf = tempFiles[rId]
			}
			err = json.NewEncoder(tf).Encode(&kv)
			if err != nil {
				panic(err)
			}
		}
		argsF := MapArg{}
		replyF := MapReply{}
		argsF.ID = reply.ID
		argsF.Filename = reply.Filename
		ok := call("Coordinator.MapTaskFinish", &argsF, &replyF)
		if !ok {
			for _, tf := range tempFiles {
				os.Remove(tf.Name())
			}
		} else {
			for rid, tf := range tempFiles {
				os.Rename(tf.Name(), fmt.Sprintf("mr-%d-%d", reply.ID, rid))
				tf.Close()
			}
		}
	}

	return nil
}

func CallReduceTask(reducef func(string, []string) string) error {
	for {
		args := ReduceArg{}
		reply := ReduceReply{}

		call("Coordinator.AskReduceTask", &args, &reply)
		if reply.ID == -2 {
			break
		}
		if reply.ID == -1 {
			time.Sleep(10 * time.Second)
			continue
		}
		var kva []KeyValue
		for i := 0; i < reply.NMap; i++ {
			filename := fmt.Sprintf("mr-%d-%d", i, reply.ID)
			_, err := os.Stat(filename)
			if err != nil {
				continue
			}
			Outfile, err := os.Open(filename)
			if err != nil {
				panic(err)
			}
			dec := json.NewDecoder(Outfile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			Outfile.Close()
		}

		sort.Sort(ByKey(kva))
		outFile, _ := ioutil.TempFile("./", "mr-out-*")
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

			fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
			i = j
		}
		argsF := ReduceArg{}
		replyF := ReduceReply{}
		argsF.ID = reply.ID
		if !call("Coordinator.ReduceTaskFinish", &argsF, &replyF) {
			os.Remove(outFile.Name())
		} else {
			os.Rename(outFile.Name(), fmt.Sprintf("mr-out-%d", reply.ID))
			outFile.Close()
		}
	}

	return nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
