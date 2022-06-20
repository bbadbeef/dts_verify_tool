package taskmanager

import (
	"mongo_compare/compare"
	"mongo_compare/utils"
	"os"
	"sync"
)

var (
	mu    sync.Mutex
	tasks = make(map[string]*compare.Task)
)

// AddTask ...
func AddTask(t *compare.Task) {
	utils.RegisterExitFunc(func() {
		t.SetStatus(compare.StatusTerminated)
		os.Stdout.WriteString(t.Display())
	})
	mu.Lock()
	tasks[t.Id()] = t
	mu.Unlock()
}

// GetTask ...
func GetTask(id string) *compare.Task {
	mu.Lock()
	defer mu.Unlock()
	if t, ok := tasks[id]; ok {
		return t
	}
	if len(id) == 0 {
		for _, t := range tasks {
			return t
		}
	}
	return nil
}
