package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var wg sync.WaitGroup
	wg.Add(ntasks)

	for i := 0; i < ntasks; i++ {
		if phase == mapPhase {
			args := DoTaskArgs{JobName: jobName, File: mapFiles[i], Phase: mapPhase, TaskNumber: i, NumOtherPhase: n_other}
			go assignTask(registerChan, args, &wg)
		} else {
			args := DoTaskArgs{JobName: jobName, File: "", Phase: reducePhase, TaskNumber: i, NumOtherPhase: n_other}
			go assignTask(registerChan, args, &wg)
		}

	}
	fmt.Printf("Schedule: %v waiting for all tasks to finish\n", phase)
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}

func assignTask(workers chan string, args DoTaskArgs, wg *sync.WaitGroup) {
	for {
		debug("Schedule: %v #%v waiting for worker\n", args.Phase, args.TaskNumber)
		worker := <- workers
		debug("Schedule: %v #%v assigned to worker %v\n", args.Phase, args.TaskNumber, worker)
		if call(worker, "Worker.DoTask", args, nil) {
			debug("Schedule: %v #%v done, calling waitGroup Done()\n", args.Phase, args.TaskNumber)
			wg.Done()
			debug("Schedule: worker back in queue %v\n", worker)
			workers <- worker
			break
		}
		debug("Schedule: %v #%v failed, worker no reply %v\n", args.Phase, args.TaskNumber, worker)
	}
}
