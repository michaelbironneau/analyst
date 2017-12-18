package engine

import (
	"fmt"
	"sync"
)

//Sequencer is a synchronization utility to ensure that a collection
//of named tasks run in a given sequence even if they are started in
//parallel.
type Sequencer interface {
	Wait(task string)
	Done(task string)
}

//sequencer is the default implementation of Sequencer
type sequencer struct {
	tasks map[string]int
	locks []sync.Mutex
}

func NewSequencer(tasks []string) Sequencer {
	var locks []sync.Mutex
	taskMap := make(map[string]int)
	for i := range tasks {
		taskMap[tasks[i]] = i
		locks = append(locks, sync.Mutex{})
		if i > 0 {
			locks[i].Lock()
		}
	}
	return &sequencer{
		taskMap,
		locks,
	}
}

func (s *sequencer) Done(task string) {
	if i, ok := s.tasks[task]; !ok {
		panic(fmt.Sprintf("task not found in sequencer: %s", task))
	} else {
		if i == len(s.locks)-1 {
			return //last task - no next task to unlock
		}
		s.locks[i+1].Unlock()
	}
}

func (s *sequencer) Wait(task string) {
	if i, ok := s.tasks[task]; !ok {
		panic(fmt.Sprintf("task not found in sequencer: %s", task))
	} else {
		s.locks[i].Lock()
		s.locks[i].Unlock() //to make idempotent
	}
}
