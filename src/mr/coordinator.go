package mr

//import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv" //to convert int to char
import "sync"  // for synchronization mechanisms
import "time"  // for ticker, so that the coordinator cana monitor can the worker
  


const (
	UnAssigned = iota
	Assigned
	Finished
)

var maptasks chan string          // chan for map tasks
var reducetasks chan int  		// channel for reduce tasks



type Coordinator struct {

	MapJobStatus		map[string]int
	ReduceJobStatus	map[int]int
	MapJobNum		int
	ReduceJobNum		int
	InterFiles		[][]string
	MapFin			bool
	RedFin			bool
	mu			sync.Mutex	
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RPCHandler(args *Args,reply *Reply) error {
	
	// This functions listens to the requests and recieves them from workers
	//If the worker requests a Mapper or Reducer job, checks if any request is waiting in the queue and assigns it
	// If the worker sends a message of completion of Map job, it updates the status of the Map jobs done till now
	/// If the worker sends a message of completion of Reduce job, it updates the status of the Map jobs done till now
	MsgType := args.MsgType
	switch(MsgType) {
	case "MsgForJob":
		
		select {
		case filename := <- maptasks:
			reply.Filename=filename
			reply.MapNum=c.MapJobNum
			reply.RedNum=c.ReduceJobNum
			reply.TaskType="map"
			c.mu.Lock()
			c.MapJobStatus[filename]=Assigned
			c.MapJobNum++
			c.mu.Unlock()
			go c.MonitorWorker("map",filename)
			
		
		case ReduceJobNum := <- reducetasks:
			reply.TaskType="reduce"
			reply.RedFileList=c.InterFiles[ReduceJobNum]
			reply.RedNum=c.ReduceJobNum
			reply.RedNum=ReduceJobNum
			
			c.mu.Lock()
			c.ReduceJobStatus[ReduceJobNum]=Assigned
			c.mu.Unlock()
			go c.MonitorWorker("reduce",strconv.Itoa(ReduceJobNum))
			return nil
		}
	case "MsgForFinishMap":
		c.mu.Lock()
		c.MapJobStatus[args.MsgCnt]=Finished
		c.mu.Unlock()
	case "MsgForFinishReduce":
		index, _ := strconv.Atoi(args.MsgCnt)
		c.mu.Lock()
		c.ReduceJobStatus[index]=Finished
		c.mu.Unlock()
	
	}
	return nil	
}

func (c *Coordinator) MonitorWorker(taskType,identifier string) {
	
	//In order to assign a task to some other mapper or reducers if one is down, the coordinator keeps check on the mappers and reducers who have taken more than 10 seconds and assigns it to the other one available.
	// If the job is done within 10 seconds, it updates the status of number of map jobs or reduce jobs
	ticker := time.NewTicker(10*time.Second)
	defer ticker.Stop()
	for {
		select{
		case <- ticker.C:
			if taskType =="map"{
				c.mu.Lock()
				c.MapJobStatus[identifier]=UnAssigned
				c.mu.Unlock()
				maptasks <- identifier
			} else if taskType == "reduce"{
				index,_:=strconv.Atoi(identifier)
				c.mu.Lock()
				c.ReduceJobStatus[index]=UnAssigned
				c.mu.Unlock()
				reducetasks <- index
			}
			return
			
		default:
			if taskType == "map" {
				c.mu.Lock()
				if c.MapJobStatus[identifier]==Finished {
					c.mu.Unlock()
					return
				} else {
					c.mu.Unlock()
				}
			} else if taskType == "reduce" {
				index, _ :=strconv.Atoi(identifier)
				c.mu.Lock()
				if c.ReduceJobStatus[index]== Finished {
					c.mu.Unlock()
					return
				} else {
					c.mu.Unlock()
				}
			}
		}
	}
}

// Recieves the Inter mediate files from the map jobs which now need to be assigned to the reducers
func (c *Coordinator) InterFileHandler(args *InterFile,reply *Reply) error {

	ReduceNum := args.RedType
	filename := string(args.MsgCnt)
	c.InterFiles[ReduceNum]=append(c.InterFiles[ReduceNum],filename)
	return nil
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


func (c *Coordinator) server() {

	maptasks = make(chan string,5)
	reducetasks=make(chan int,5)
	
	rpc.Register(c)
	rpc.HandleHTTP()
	
	go c.createJob()
	
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Assigns job to mappers and producers according to the worker requests
// If all jobs are done, all map and reduce jobs, Signals -> Done by making the ReduceFinish variable or RedFin variable true
func (c *Coordinator) createJob() {
	
	for k,v := range c.MapJobStatus {
		if v == UnAssigned {
			maptasks <- k
		}
	}
	
	ok := false
	for !ok {
		ok =checkAllMapJobs(c)
	}
	c.MapFin = true
	
	for k,v := range c.ReduceJobStatus {
		if v == UnAssigned {
			reducetasks <- k
		}
	}
	
	ok = false
	for !ok {
		ok=checkAllReduceJobs(c)
	}
	c.RedFin=true
}

// If all map jobs are done, send a signal to the createJob function
func checkAllMapJobs(c *Coordinator) bool {

	c.mu.Lock()
	defer c.mu.Unlock()
	for _,v := range c.MapJobStatus {
		if v!=Finished {
			return false
		}
	}
	
	return true
}
// If all reduce jobs are done, send a signal to the createJob function
func checkAllReduceJobs(c *Coordinator) bool {

	c.mu.Lock()
	defer c.mu.Unlock()
	for _,v := range c.ReduceJobStatus {
		if v!=Finished {
			return false
		}
	}
	
	return true
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
//If the createJobs gets a green signal from both the CheckALlReduceJobs and CheckAllMapJobs fucntions, it return true to the mrcoordinator.go
func (c *Coordinator) Done() bool {
	ret := true

	ret = c.RedFin

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MapJobStatus=make(map[string]int)
	c.MapJobNum=0
	c.ReduceJobNum=nReduce
	c.MapFin=false
	c.RedFin=false
	c.ReduceJobStatus=make(map[int]int)
	c.InterFiles=make([][]string,c.ReduceJobNum)
	//mu sync.Mutex
	//c.mu = new(sync.Mutex)
	
	for _,v := range files {
		c.MapJobStatus[v]=UnAssigned
	}
	for i:=0; i<nReduce ; i++ {
		c.ReduceJobStatus[i]=UnAssigned
	}


	c.server()
	return &c
}
