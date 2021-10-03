package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "sort"

//
// Map functions return a slice of KeyValue.
//

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


type KeyValue struct {
	Key   string
	Value string
}

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
	
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	
	for(true){
		reply := AskForJob("MsgForJob","")
		if(reply.TaskType == ""){
			break
		}
		switch(reply.TaskType) {
		case "map":
			mapWorker(&reply,mapf)
		case "reduce":
			reduceWorker(&reply,reducef)
		}
	}
}

func mapWorker(reply *Reply,mapf func(string,string) []KeyValue) {
	
	file,err := os.Open(reply.Filename)
	if err !=nil {
		log.Fatalf("cannot open %v",reply.Filename)
	}
	content,err := ioutil.ReadAll(file)
	if err !=nil {
		log.Fatalf("cannot read %v",reply.Filename)
	}
	kva := mapf(reply.Filename,string(content))
	kvas:=Partition(kva,reply.Reducers)
	
	for i:=0;i<reply.Reducers;i++ {
		filename := WriteToJSONFile(kvas[i],reply.MapNum,i)
		_ = SendInterFiles(MsgForInterFileLoc,filename,string(i))
	}
	_ = AskForJob(MsgForFinishMap,reply.Filename)
	
	file.Close()
}

func reduceWorker(reply *Reply,reducef func(string,[]string) string) {
	
	intermediate := []KeyValue{}
	for _,v := range reply.RedFileList {
		file,err := os.Open(v)
		if err !=nil {
			log.Fatalf("cannot open %v",v)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err!=nil {
				break
			}
			intermediate=append(intermediate,kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(reply.RedNum)
	ofile,_:=os.Create(oname)
	
	i := 0
	for i<len(intermediate) {
		j := i+1
		for j<len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values:=[]string{}
		for k := i;k<j;k++ {
			values=append(values,intermediate[k].Value)
		}
		output:=reducef(intermediate[i].Key,values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	_ = AskForJob(MsgForFinishReduce, strconv.Itoa(reply.RedNum))
}


func AskForJob(msgType string,msgCnt string) Reply {
	
	args:=Args{}
	args.MsgType=msgType
	args.MsgCnt=msgCnt	
	reply:=Reply{}
	reply.Reducers=4
	res:=call("Coordinator.RPCHandler",&args,&reply)
	if !res {
		return Reply{TaskType:""}
	}
	return reply
}


func SendInterFiles(msgType string,msgCnt string, nReduceType string ) Reply {
	
	args:=InterFile{}
	
	args.MsgType,_=strconv.Atoi(msgType)
	args.MsgCnt,_= strconv.Atoi(msgCnt)
	args.RedType,_=strconv.Atoi(nReduceType)
	reply:=Reply{}

	res := call("Coordinator.InterFileHandler",&args,&reply)
	if !res {
		fmt.Println("error sending intermediate files' location")
	}
	return reply
}
//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
	return false
}

func WriteToJSONFile(intermediate []KeyValue,mapTaskNum,reduceTaskNum int)string {
	
	filename := "mr-"+strconv.Itoa(mapTaskNum)+"-"+strconv.Itoa(reduceTaskNum)
	jfile, _ := os.Create(filename)

	enc := json.NewEncoder(jfile)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if(err != nil) {
			log.Fatal("error: ",err)
		}
	}
	return filename	
}

func WriteToReduceOutput(key, values string, nReduce int) {
	
	filename := "mr-out-"+strconv.Itoa(nReduce)
	ofile, err := os.Open(filename)
	if err != nil {
		fmt.Println("no such file")
		ofile, _ = os.Create(filename)
	}

	fmt.Fprintf(ofile, "%v %v\n", key, values)
}


// Partition : divide intermedia keyvalue pairs into nReduce buckets
func Partition(kva []KeyValue, nReduce int) [][]KeyValue {
	
	kvas := make([][]KeyValue,nReduce)
	for _,kv := range kva {
		v := ihash(kv.Key) % nReduce
		kvas[v] = append(kvas[v], kv)
	}
	return kvas
}
