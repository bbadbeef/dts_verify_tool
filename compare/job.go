package compare

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

// Job ...
type Job interface {
	setBase(*BaseJob)
	init() error
	polymorphism()
	do() (bool, error)
	name() string
}

// BaseJob ...
type BaseJob struct {
	parameter *Parameter
	log       *logrus.Logger
	r         *record
	e         error
	task      *Task
	tName     string

	srcClient       *mongo.Client
	dstClient       *mongo.Client
	srcMongodClient []*mongo.Client

	ctx    context.Context
	cancel context.CancelFunc

	imData map[string]interface{}
}

func (bj *BaseJob) init() error {
	return nil
}

func (bj *BaseJob) polymorphism() {}

func (bj *BaseJob) setData(k string, v interface{}) {
	bj.imData[k] = v
}

func (bj *BaseJob) getData(k string) interface{} {
	return bj.imData[k]
}

func (bj *BaseJob) setError(err error) {
	bj.e = err
}

func (bj *BaseJob) error() error {
	return bj.e
}

func (bj *BaseJob) notifyDiff(t int, a int, items []*MetaDiffItem) {
	if bj.parameter.CBFunc == nil {
		return
	}
	if t == Data {
		return
	}
	res := make([]*DiffRecord, 0)
	for _, item := range items {
		res = append(res, &DiffRecord{
			Typ:    NotifyTypeName[t],
			Action: a,
			Ns:     item.Ns,
			SrcId:  marshalId(item.SrcId),
			DstId:  marshalId(item.DstId),
			SrcVal: marshalVal(item.SrcVal),
			DstVal: marshalVal(item.DstVal),
		})
	}
	if len(res) != 0 {
		bj.parameter.CBFunc(&Event{
			EventType: EventDiffRecord,
			Data:      res,
		})
	}
}

func (bj *BaseJob) updateStepSimple(step string) error {
	return bj.r.updateStatus(bj.task.id, map[string]interface{}{
		"sub_task": bj.tName,
		"step":     step,
	})
}

func (bj *BaseJob) updateStep(step string) error {
	return bj.r.updateStatus(bj.task.id, map[string]interface{}{
		"sub_task":      bj.tName,
		"step":          step,
		"progress":      0,
		"finish_ns":     []string{},
		"finish_ns_cnt": "",
	})
}

type assistJob struct {
	*BaseJob
}

func (aj *assistJob) setBase(b *BaseJob) {
	aj.BaseJob = b
}

type initJob struct {
	assistJob
}

func (ij *initJob) do() (bool, error) {
	ij.tName = ij.task.name
	ij.imData = make(map[string]interface{})
	var err error
	ij.srcClient, err = newMongoClient(ij.parameter.SrcUrl)
	if err != nil {
		ij.log.Errorf("new src client error: %s", err.Error())
		return false, err
	}
	ij.dstClient, err = newMongoClient(ij.parameter.DstUrl)
	if err != nil {
		ij.log.Errorf("new dst client error: %s", err.Error())
		return false, err
	}

	for _, uri := range ij.parameter.SrcMongodUrl {
		client, err := newMongoClient(uri)
		if err != nil {
			ij.log.Errorf("new mongod client error: %s, %s", uri, err.Error())
			return false, err
		}
		ij.srcMongodClient = append(ij.srcMongodClient, client)
	}
	if len(ij.srcMongodClient) == 0 {
		ij.srcMongodClient = append(ij.srcMongodClient, ij.srcClient)
	}

	ij.r = &record{
		srcClient: ij.srcClient,
		dstClient: ij.dstClient,
		outDb:     ij.parameter.ResultDb,
		taskId:    ij.task.id,
	}

	switch ij.parameter.RunMode {
	case "resume":
		status, err := ij.r.GetStatus()
		if err != nil {
			return false, err
		}
		if status == nil {
			return false, fmt.Errorf("not found resume info")
		}
		if err := ij.setResumeInfo(status); err != nil {
			ij.log.Errorf("set resume info error: %s", err.Error())
			return false, err
		}
	case "compare":
		if err := ij.r.init(); err != nil {
			ij.log.Errorf("init result db error: %s", err.Error())
			return false, err
		}

		if err := ij.r.saveStatus(&TaskStatus{
			Name:      ij.task.name,
			Extra:     ij.parameter.CompareExtra,
			SubTask:   ij.tName,
			OId:       ij.task.id,
			Status:    StatusRunning,
			Step:      "init",
			StartTime: time.Now().Format("2006-01-02 15:04:05"),
		}); err != nil {
			return false, err
		}
	}

	ij.ctx, ij.cancel = context.WithCancel(context.Background())

	ij.log.Info("init ok")
	return true, nil
}

func (ij *initJob) setResumeInfo(status *TaskStatus) error {
	ij.tName = status.Name
	ij.task.name = status.Name
	ij.task.id = status.OId
	ij.parameter.CompareType = status.Name
	ij.parameter.CompareExtra = status.Extra
	switch status.Step {
	case "staticDataJob":
		if err := ij.r.cleanForResume(); err != nil {
			return err
		}
		ij.setData("ts", status.Ts)
		ij.setData("finishedNs", status.FinishNs)
	case "dynamicDataJob":
		ij.setData("ts", status.Ts)
		ij.task.steps = []Job{&dynamicDataJob{assistJob: ij.assistJob}}
	default:
		break
	}
	if err := ij.r.updateStatus(ij.task.id, map[string]interface{}{
		"status": StatusRunning,
		"error":  "",
	}); err != nil {
		ij.log.Errorf("update status error: %s", err.Error())
		return err
	}
	return nil
}
