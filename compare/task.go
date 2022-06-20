package compare

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"mongo_compare/utils"
)

var (
	TaskStep = map[string][]Job{
		"account":            {&nsJob{}, &nsFilterAccountJob{}, &countCompareJob{}, &accountDataJob{}},
		"javascript":         {&nsJob{}, &nsFilterJavascriptJob{}, &countCompareJob{}, &javascriptDataJob{}},
		"shard_key":          {&nsFilterShardKeyJob{}, &countCompareJob{}, &shardKeyDataJob{}},
		"tag":                {&nsFilterTagJob{}, &countCompareJob{}, &tagDataJob{}},
		"data_count_preview": {&nsJob{}, &nsFilterBaseJob{}, &countCompareJob{}},
		"data_count":         {&nsJob{}, &nsFilterBaseJob{}, &countCompareJob{}},
		"data_all_content":   {&nsJob{}, &nsFilterBaseJob{}, &countCompareJob{}, &staticDataJob{}},
		"data_all":           {&nsJob{}, &nsFilterBaseJob{}, &countCompareJob{}, &staticDataJob{}, &dynamicDataJob{}},
		"data_db_content":    {&nsJob{}, &nsFilterSpecifiedJob{}, &countCompareJob{}, &staticDataJob{}},
		"data_db_all":        {&nsJob{}, &nsFilterSpecifiedJob{}, &countCompareJob{}, &staticDataJob{}, &dynamicDataJob{}},
		"all": {&nsJob{}, &nsFilterAccountJob{}, &countCompareJob{}, &accountDataJob{},
			&nsFilterJavascriptJob{}, &countCompareJob{}, &javascriptDataJob{},
			&nsFilterShardKeyJob{}, &countCompareJob{}, &shardKeyDataJob{},
			&nsFilterTagJob{}, &countCompareJob{}, &tagDataJob{},
			&nsFilterBaseJob{}, &countCompareJob{}, &staticDataJob{}, &dynamicDataJob{}},
	}
)

// Task ...
type Task struct {
	id   string
	name string
	l    *logrus.Logger
	p    *Parameter
	b    *BaseJob

	steps   []Job
	current int
}

// NewTask ...
func NewTask(l *logrus.Logger, para *Parameter) *Task {
	id := utils.GenTaskId()
	l.Infof("create task: %s", id)

	return &Task{id: id, l: l, p: para}
}

// Id ...
func (t *Task) Id() string {
	return t.id
}

// Para ...
func (t *Task) Para() *Parameter {
	return t.p
}

// SetConcurrency ...
func (t *Task) SetConcurrency(src, dst int) {
	if src != 0 {
		t.p.SrcConcurrency = src
	}
	if dst != 0 {
		t.p.DstConcurrency = dst
	}
	t.p.dirty = true
}

// Init ...
func (t *Task) Init() error {
	t.current = -1
	t.name = t.p.CompareType
	systemDb = append(systemDb, "TencetDTSData")
	systemDb = append(systemDb, t.p.ResultDb)

	base := &BaseJob{parameter: t.p, log: t.l, task: t}
	t.b = base

	ij := &initJob{assistJob{base}}
	b, err := ij.do()
	if err != nil {
		return err
	}
	if !b {
		return fmt.Errorf("init task error, init job failed")
	}

	if len(t.steps) != 0 {
		return nil
	}
	for _, s := range TaskStep[t.p.CompareType] {
		s.setBase(base)
		t.steps = append(t.steps, s)
	}

	return nil
}

// Run ...
func (t *Task) Run() error {
	for _, s := range t.steps {
		t.current++
		s.polymorphism()
		if err := s.init(); err != nil {
			t.l.Errorf("error occurred while init step %s, error: %s", s.name(), err.Error())
			if err := t.b.r.setTaskError(t.id, err); err != nil {
				t.l.Errorf("save result error: %s", err.Error())
			}
			return err
		}

		b, err := s.do()
		if err != nil {
			t.l.Errorf("error occurred in step %s, error: %s", s.name(), err.Error())
			if err := t.b.r.setTaskError(t.id, err); err != nil {
				t.l.Errorf("save result error: %s", err.Error())
			}
			return err
		}
		if !b {
			t.l.Errorf("task terminated, step %s", s.name())
			return fmt.Errorf("step %s compare not equal", s.name())
		}
	}
	return nil
}

// SetStatus ...
func (t *Task) SetStatus(s string) {
	if err := t.b.r.updateStatus(t.id, map[string]interface{}{
		"status": s,
	}); err != nil {
		t.l.Errorf("update status error: %s", err.Error())
	}
}

// Display ...
func (t *Task) Display() string {
	if t.b == nil || t.b.r == nil {
		return "inner error"
	}
	return getTaskStatusByRecord(t.b.r)
}

// DisplayConfig ...
func (t *Task) DisplayConfig() string {
	if t.p == nil {
		return "failed"
	}
	return t.p.display()
}
