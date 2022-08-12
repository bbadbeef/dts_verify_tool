package compare

import (
	"context"
	"fmt"
	"github.com/bbadbeef/dts_verify_tool/utils"
	"github.com/sirupsen/logrus"
	"time"
)

const (
	Account = 1 << iota
	Index
	ShardKey
	Tag
	Js
	Namespace
	Count
	Data
	Partial
)

func genJobs(c uint) []Job {
	jobs := []Job{&nsJob{}}
	metaJobs := []Job{&nsJob{}}
	if c&Account != 0 {
		metaJobs = append(metaJobs, &nsFilterAccountJob{}, &countCompareJob{}, &accountDataJob{})
	}
	if c&ShardKey != 0 {
		metaJobs = append(metaJobs, &nsFilterShardKeyJob{}, &countCompareJob{}, &shardKeyDataJob{})
	}
	if c&Tag != 0 {
		metaJobs = append(metaJobs, &nsFilterTagJob{}, &countCompareJob{}, &tagDataJob{})
	}
	if c&Js != 0 {
		metaJobs = append(metaJobs, &nsFilterJavascriptJob{}, &countCompareJob{}, &javascriptDataJob{})
	}
	getFilter := func() Job {
		if c&Partial != 0 {
			return &nsFilterSpecifiedJob{}
		}
		return &nsFilterBaseJob{}
	}
	if c&Index != 0 {
		metaJobs = append(metaJobs, getFilter(), &indexJob{})
	}
	if c&Namespace != 0 || c&Count != 0 {
		metaJobs = append(metaJobs, getFilter(), &countCompareJob{})
	}
	if c&Data != 0 {
		jobs = append(jobs, getFilter(), &countCompareJob{}, &staticDataJob{}, &dynamicDataJob{})
	}
	jobs = append(jobs, &toFinishingJob{})
	jobs = append(jobs, metaJobs...)

	return jobs
}

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

		"customize": {},
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
	status  string

	onFinishFunc []func()
}

// NewTask ...
func NewTask(para *Parameter) *Task {
	w, err := utils.GetRotateWriter()
	if err != nil {
		return nil
	}
	l := utils.NewLogger(w, para.Id)
	l.Infof("create task: %s", para.Id)

	return &Task{id: para.Id, l: l, p: para}
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

	steps := TaskStep[t.p.CompareType]
	if len(steps) == 0 {
		steps = genJobs(t.p.CompareExtra)
	}
	for _, s := range steps {
		s.setBase(base)
		t.steps = append(t.steps, s)
	}

	return nil
}

// Run ...
func (t *Task) Run() (err error) {
	t.status = StatusRunning

	defer func() {
		fields := map[string]interface{}{
			"status":   StatusSuccess,
			"end_time": time.Now().Format("2006-01-02 15:04:05"),
		}
		if err != nil {
			fields["status"] = StatusFailed
			fields["error"] = err.Error()
		}
		if t.status == StatusTerminated {
			fields["status"] = StatusTerminated
		}
		if e := t.b.r.updateStatus(t.id, fields); e != nil {
			t.l.Errorf("save result error: %s", err.Error())
		}
		for _, f := range t.onFinishFunc {
			f()
		}
		t.clean()
	}()
	for _, s := range t.steps {
		if t.status != StatusRunning {
			return nil
		}
		t.current++
		s.polymorphism()
		if err = s.init(); err != nil {
			t.l.Errorf("error occurred while init step %s, error: %s", s.name(), err.Error())
			return err
		}

		_, err = s.do()
		if err != nil {
			t.l.Errorf("error occurred in step %s, error: %s", s.name(), err.Error())
			return err
		}
		//if !b {
		//	t.l.Errorf("task terminated, step %s", s.name())
		//	return fmt.Errorf("step %s compare not equal", s.name())
		//}
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

// Terminate ...
func (t *Task) Terminate() error {
	t.status = StatusTerminated
	t.b.cancel()
	return nil
}

// SetCBFunc ...
func (t *Task) SetCBFunc(f func(event *Event)) {
	t.p.CBFunc = f
}

// SetFiniteFunc ...
func (t *Task) SetFiniteFunc(f func() bool) {
	t.p.FiniteFunc = f
}

// GetStatus ...
func (t *Task) GetStatus() (*TaskStatus, error) {
	return t.b.r.GetStatus()
}

// GetOplogDelay ...
func (t *Task) GetOplogDelay() (int, error) {
	return t.b.r.getOplogDelay()
}

// AddActionOnFinish ...
func (t *Task) AddActionOnFinish(f func()) {
	t.onFinishFunc = append(t.onFinishFunc, f)
}

// GetLog ...
func (t *Task) GetLog() *logrus.Logger {
	return t.l
}

// GetSampleDiffData ...
func (t *Task) GetSampleDiffData() ([]*MetaDiffItem, error) {
	return t.b.r.GetSampleDiffData()
}

func (t *Task) clean() {
	t.b.srcClient.Disconnect(context.Background())
	t.b.dstClient.Disconnect(context.Background())
	for _, c := range t.b.srcMongodClient {
		c.Disconnect(context.Background())
	}
}
