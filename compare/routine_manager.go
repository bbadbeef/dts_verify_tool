package compare

import (
	"sort"
	"sync"
)

type routineUint struct {
	exitCh       chan struct{}
	priorityFunc func() int
}

func newRoutineUnit() *routineUint {
	return &routineUint{
		exitCh: make(chan struct{}, 1),
	}
}

func (ru *routineUint) setPriorityFunc(f func() int) {
	ru.priorityFunc = f
}

func (ru *routineUint) shouldExit() bool {
	select {
	case <-ru.exitCh:
		return true
	default:
		return false
	}
}

type routineManager struct {
	sync.Mutex
	routines []*routineUint
}

func (rm *routineManager) add(r *routineUint) {
	rm.Lock()
	rm.routines = append(rm.routines, r)
	rm.Unlock()
}

func (rm *routineManager) remove(r *routineUint) {
	rm.Lock()
	for i, u := range rm.routines {
		if u == r {
			rm.routines = append(rm.routines[:i], rm.routines[i+1:]...)
			break
		}
	}
	rm.Unlock()
}

func (rm *routineManager) sort() {
	sort.Slice(rm.routines, func(i, j int) bool {
		if rm.routines[i].priorityFunc == nil || rm.routines[j].priorityFunc == nil {
			return true
		}
		return rm.routines[i].priorityFunc() < rm.routines[j].priorityFunc()
	})
}

func (rm *routineManager) len() int {
	rm.Lock()
	defer rm.Unlock()
	return len(rm.routines)
}

func (rm *routineManager) destroyN(n int) {
	rm.Lock()
	defer rm.Unlock()
	i := 0
	for _, r := range rm.routines {
		if i == n {
			break
		}
		i++
		r.exitCh <- struct{}{}
	}
	rm.routines = rm.routines[i:]
}
