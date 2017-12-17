package engine

import (
	. "github.com/smartystreets/goconvey/convey"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestSequencer(t *testing.T) {
	c := make(chan int, 3)
	var wg sync.WaitGroup
	Convey("Given a sequencer and a list of tasks", t, func() {
		tasks := []string{"a", "b", "c"}
		rand.Seed(time.Now().Unix())
		perm := rand.Perm(3)
		s := NewSequencer(tasks)
		Convey("It should execute the tasks in the correct order", func() {
			for i := range tasks {
				wg.Add(1)
				go func(name string, result int) {
					s.Wait(name)
					c <- result
					s.Done(name)
					wg.Done()
				}(tasks[perm[i]], perm[i])
			}
			wg.Wait()
			close(c)
			exp := []int{0, 1, 2}
			var act []int
			for i := range c {
				act = append(act, i)
			}
			So(exp, ShouldResemble, act)
		})

	})
}
