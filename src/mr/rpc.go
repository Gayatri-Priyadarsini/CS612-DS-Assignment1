package mr


import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

const MsgForJob = "MsgForJob" 
const MsgForInterFileLoc = "MsgForInterFileLoc" 
const MsgForFinishMap  = "MsgForFinishMap"
const MsgForFinishReduce = "MsgForFinishReduce"


type Args struct {
	MsgType 	string	//Indicates which type of msg, Msg for job request, file location, or for signalling that map or reduce job is over/finished
	MsgCnt		string
}

type Reply struct {
	Filename	string	
	MapNum	int
	RedNum	int
	Reducers 	int
	TaskType	string
	RedFileList	[]string
	
}


type InterFile struct {
	MsgType	int
	MsgCnt	int
	RedType	int
}
func coordinatorSock() string {
	s := "/var/tmp/cs612-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
