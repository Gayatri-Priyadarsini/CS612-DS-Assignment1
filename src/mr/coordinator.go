package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv" //to convert int to char
import "sync"  // for synchronization mechanisms
import "time"  // for ticker, so that the coordinator cana monitor can the worker
 


type Coordinator struct {
	// Your definitions here.
	MapJobStatus	map[string]int
	ReduceJobStatus	map[int]int
	MapJobNum	int
	ReduceJobNum	int
	InterFiles	[][]string
	MapFin	bool
	RedFin	bool
	mu	*sync.Mutex	
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RPCHandler(args *Args,reply *Reply) error 
{
	fmt.Println("RPC Handler is called by the worker to communicate with the coordinator")
	MsgType := args.MsgType
	switch(MsgType)
	{
	case MsgForJob:
		select 
		{
		case filename := <- map_job_channel:
			reply.Filename=filename
			reply.MapNum=c.MapJobNum
			reply.Reducers=c.ReduceJobNum
			reply.TaskType="map"
			c.mu.Lock()
			c.MapJobStatus[filename]=Assigned
			c.MapJobNum++
			c.mu.Unlock()
			go c.MonitorWorker("map",filename)
			return nil
		
		case ReduceJobNum := ,- reduce_job_channel:
			reply.TaskType="reduce"
			reply.RedFileList=c.InterFiles[ReduceJobNum]
			reply.Reducers=c.ReduceJobNum
			reply.RedNum=c.ReduceJobNum
			
			c.mu.Lock()
			c.ReduceJobStatus[ReduceJobNum]=Assigned
			c.mu.Unlock()
			go c.MonitorWorker("reduce",strconv.Itoa(ReduceJobNum))
			return nil
		}
	case MsgForFinishMap:
		c.mu.Lock()
		c.MapJobStatus[args.MsgCnt]=Finished
		c.mu.Unlock()
	case MsgForFinishReduce:
		index, _ := strconv.Atoi(args.MessageCnt)
		c.mu.Lock()
		c.ReduceJobStatus[index]=Finished
	}
	return nil
	
}

func (c *Coordinator) MonitorWorker(taskType,identifier string)
{
	ticker := time.NewTicker(10*time.Second)
	for 
	{
		select
		{
		case <- ticker.C:
			if taskType =="map"
			{
				c.mu.Lock()
				c.MapJobStatus[identifier]=NotAssigned
				c.mu.Unlock()
				maptasks <- identifier
			}
			else if taskType == "reduce"
			{
				index,_:=strconv.Atoi(identifier)
				c.mu.Lock()
				c.ReduceTaskStatus[index]=UnAssigned
				c.mu.Unlock()
				reducetasks <- index
			}
			return 
		
		default:
			if taskType == "map"
			{
				c.mu.Lock()
				if c.MapJobStatus[identifier]==Finished
				{
					c.mu.Unlock()
					return
				} 
				else
				{
					c.mu.Unlock()
				}
			}
			else if taskType == "reduce"
			{
				index, _ :=strconv.Atoi(identifier)
				c.mu.Lock()
				if c.ReduceJobStatus[index]== Finished 
				{
					c.mu.Unlock()
					return
				}
				else
				{
					c.mu.Unlock()
				}
			}

		}
	}
	ticker.Stop()
}


func (c *Coordinator) InterFileHandler(args *InterFile,reply *Reply)
{
	ReduceNum := args.RedType;
	filename := args.MsgCnt;
	c.InterFiles[ReduceNum]=append(c.InterFiles[ReduceNum],filename)
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	
	maptasks = make(chan string,5)
	reducetasks=make(chan int,5)
	
	rpc.Register(c)
	rpc.HandleHTTP()
	
	go m.createJob()
	
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}


func (c *Coordinator) createJob()
{
	
	for k,v := range c.MapJobStatus
	{
		if v==UnFinished
		{
			maptasks <- k
		}
	}
	
	ok=false
	for !ok 
	{
		ok =checkAllMapJobs(c)
	}
	c.MapFin=true
	
	for k,v := range c.ReduceJobStatus
	{
		if v==UnFinished
		{
			reducetasks <- k
		}
	}
	
	ok=false
	for !ok 
	{
		ok =checkAllReduceJobs(c)
	}
	c.RedFin=true
}

func checkAllMapJobs(c *Coordinator) bool
{
	c.mu.Lock()
	for _,v := range c.MapJobStatus
	{
		if v!=Finished
		{
			return false
		}
	}
	c.mu.Unlock()
	return true
}

func checkAllReduceJobs(c *Coordinator) bool
{
	c.mu.Lock()
	for _,v := range c.ReduceJobStatus
	{
		if v!=Finished
		{
			return false
		}
	}
	c.mu.Unlock()
	return true
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = m.RedFin

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
	c.Reducers=nReduce
	c.MapFin=false
	c.RedFin=false
	c.ReduceJobStatus=make(map[int]int)
	c.InterFiles=make([][]string,c.Reducers)
	c.mu=new(sync.RWMutex)
	
	for _,v := range files
	{
		c.MapJobStatus[v]=UnAssigned
	}
	for i:=0; i<nReduce ; i++
	{
		c.ReduceJobStatus[i]=UnAssigned
	}


	c.server()
	return &c
}
