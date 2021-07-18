package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus int

const (
	IDLE      TaskStatus = iota
	RUNNING   TaskStatus = iota
	COMPLETED TaskStatus = iota
)

type TaskInfo struct {
	status  TaskStatus
	wid     int
	StartTs int64
	EndTs   int64
}

type Tasks map[interface{}]*TaskInfo

type Master struct {
	WStatus           map[int]string
	MTasks            Tasks
	MapTaskNumber     int
	RTasks            Tasks
	IntermediateFiles map[int][]string
	nReducers         int
	mu                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AcquireTask(args *AcquireTaskArgs, reply *AcquireTaskReply) error {
	m.mu.Lock()

	if m.WStatus[args.Wid] == "" {
		m.WStatus[args.Wid] = "IDLE"
	}

	mapTask := m.PickMapTask(args.Wid)

	if mapTask != nil {
		reply.MapTask = mapTask
		reply.Done = false
		m.WStatus[args.Wid] = "BUSY"
		m.mu.Unlock()
		return nil
	}

	if !m.CheckAllMapTasksDone() {
		reply.Done = false
		m.WStatus[args.Wid] = "IDLE"
		m.mu.Unlock()
		return nil
	}

	reduceTask := m.PickReduceTask(args.Wid)
	if reduceTask != nil {
		reply.ReduceTask = reduceTask
		reply.Done = false
		m.WStatus[args.Wid] = "BUSY"
		m.mu.Unlock()
		return nil

	}

	m.mu.Unlock()
	reply.Done = m.Done()

	return nil

}

func (m *Master) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WStatus[args.Wid] = "IDLE"
	if m.MTasks[args.InputFile].status != COMPLETED {
		m.MTasks[args.InputFile].status = COMPLETED
		m.MTasks[args.InputFile].EndTs = time.Now().Unix()
		for i := 0; i < m.nReducers; i++ {
			m.IntermediateFiles[i] = append(m.IntermediateFiles[i], args.IntermediateFiles[i])
		}
	}

	return nil
}

func (m *Master) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *ReduceTaskDoneReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WStatus[args.Wid] = "IDLE"
	if m.RTasks[args.ReduceNumber].status != COMPLETED {
		m.RTasks[args.ReduceNumber].EndTs = time.Now().Unix()
		m.RTasks[args.ReduceNumber].status = COMPLETED
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, val := range m.RTasks {
		if val.status == IDLE || val.status == RUNNING {
			return false
		}
	}
	return true
}

func (m *Master) PickMapTask(wid int) *MapTask {
	var mapTask *MapTask = nil

	for key, val := range m.MTasks {
		filePath := key.(string)
		if val.status == IDLE {
			mapTask = &MapTask{
				Filepath:      filePath,
				MapTaskNumber: m.MapTaskNumber,
				NReducers:     m.nReducers,
			}
			m.MapTaskNumber++
			m.MTasks[filePath] = &TaskInfo{status: RUNNING, StartTs: time.Now().Unix(), wid: wid}
			break
		}
	}
	return mapTask
}

func (m *Master) CheckAllMapTasksDone() bool {
	for _, val := range m.MTasks {
		if val.status == IDLE || val.status == RUNNING {
			return false
		}
	}
	return true
}

func (m *Master) PickReduceTask(wid int) *ReduceTask {
	var reduceTask *ReduceTask = nil
	for k, val := range m.RTasks {
		if val.status == IDLE {
			reduceTask = &ReduceTask{
				IntermediateFiles: m.IntermediateFiles[k.(int)],
				ReduceNumber:      k.(int),
			}
			m.RTasks[k.(int)] = &TaskInfo{status: RUNNING, StartTs: time.Now().Unix(), wid: wid}
			break
		}
	}
	return reduceTask
}

func (m *Master) StartTaskMonitor() {
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case t := <-ticker.C:
				if m.Done() {
					return
				}
				m.checkDeadTasks(t.Unix())
			}
		}
	}()
}

func (m *Master) checkDeadTasks(checkTs int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range m.MTasks {
		if v.status == RUNNING && (v.StartTs > -1 && checkTs-v.StartTs > 10) {
			m.WStatus[m.MTasks[k].wid] = "DEAD"
			m.MTasks[k] = &TaskInfo{
				status:  IDLE,
				wid:     -1,
				StartTs: -1,
				EndTs:   -1,
			}
		}
	}

	for k, v := range m.RTasks {
		if v.status == RUNNING && (v.StartTs > -1 && checkTs-v.StartTs > 10) {
			m.WStatus[m.RTasks[k].wid] = "DEAD"
			m.RTasks[k] = &TaskInfo{
				status:  IDLE,
				wid:     -1,
				StartTs: -1,
				EndTs:   -1,
			}
		}
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{nReducers: nReduce, MTasks: Tasks{}, RTasks: Tasks{}, WStatus: make(map[int]string), IntermediateFiles: make(map[int][]string)}

	for idx, file := range files {
		if idx == 0 {
			m.MapTaskNumber = idx
		}
		m.MTasks[file] = &TaskInfo{status: IDLE, StartTs: -1, EndTs: -1}
	}

	for i := 0; i < nReduce; i++ {
		m.RTasks[i] = &TaskInfo{status: IDLE, StartTs: -1, EndTs: -1}
	}
	m.StartTaskMonitor()
	m.server()
	return &m
}
