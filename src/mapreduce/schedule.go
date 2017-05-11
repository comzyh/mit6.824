package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nOther int // number of inputs (for reduce) or outputs (for map)
	var wg sync.WaitGroup

	// // idle Worker
	// idleWorkerChan := make(chan string, 1)
	// go func() {
	// 	for worker := range registerChan {
	// 		fmt.Printf("Worker %s is ready\n", worker)
	// 		idleWorkerChan <- worker
	// 	}
	// 	fmt.Println("Close")
	// 	close(idleWorkerChan)
	// }()

	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		ntasks = nReduce
		nOther = len(mapFiles)
	}

	wg.Add(ntasks)
	for index := 0; index < ntasks; index++ {
		filename := ""
		if phase == mapPhase {
			filename = mapFiles[index]
		}
		go func(index int, filename string) {
			mapper := <-registerChan
			call(mapper, "Worker.DoTask", DoTaskArgs{
				JobName:       jobName,
				File:          filename,
				Phase:         phase,
				TaskNumber:    index,
				NumOtherPhase: nOther,
			}, nil)
			wg.Done()
			registerChan <- mapper
		}(index, filename)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nOther)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
