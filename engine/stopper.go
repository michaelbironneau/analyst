package engine

import "sync/atomic"

//Stopper is used as a condition variable stop halt the execution of the program.
//It is safe for concurrent use by multiple goroutines.
type Stopper interface{
	//Stopped checks if the stopper is stopped
	Stopped() bool

	//Stops. This is irreversible.
	Stop()
}

type stopper struct{
	flag int32
}

func (s *stopper) Stopped() bool {
	if i := atomic.LoadInt32(&s.flag); i == 1 {
		return true
	}
	return false
}

func (s *stopper) Stop() {
	atomic.StoreInt32(&s.flag, 1)
}