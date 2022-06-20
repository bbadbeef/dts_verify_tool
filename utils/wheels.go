package utils

import (
	"container/list"
	"sync"
)

// TSList thread safe list
type TSList struct {
	sync.Mutex
	l *list.List
}

// NewSTList ...
func NewSTList() *TSList {
	return &TSList{
		l: list.New(),
	}
}

// Push ...
func (sl *TSList) Push(v interface{}) {
	sl.Lock()
	sl.l.PushBack(v)
	sl.Unlock()
}

// Pop ...
func (sl *TSList) Pop() interface{} {
	sl.Lock()
	defer sl.Unlock()
	e := sl.l.Front()
	if e == nil {
		return nil
	}
	sl.l.Remove(e)
	return e.Value
}

// color
const (
	reset  = "\033[0m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	blue   = "\033[34m"
	purple = "\033[35m"
	cyan   = "\033[36m"
	gray   = "\033[37m"
	white  = "\033[97m"
)

// Red ...
func Red(str string) string {
	return red + str + reset
}

// Green ...
func Green(str string) string {
	return green + str + reset
}

// Yellow ...
func Yellow(str string) string {
	return yellow + str + reset
}

// Blue ...
func Blue(str string) string {
	return blue + str + reset
}

// Gray ...
func Gray(str string) string {
	return gray + str + reset
}
