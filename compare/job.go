package compare

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
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
	}

	switch ij.parameter.RunMode {
	case "resume":
		status, err := ij.r.getStatus()
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

		if err := ij.r.saveStatus(&taskStatus{
			Name:    ij.task.name,
			SubTask: ij.tName,
			OId:     ij.task.id,
			Status:  StatusRunning,
			Step:    "init",
		}); err != nil {
			return false, err
		}
	}

	ij.log.Info("init ok")
	return true, nil
}

func (ij *initJob) setResumeInfo(status *taskStatus) error {
	ij.tName = status.Name
	ij.task.name = status.Name
	ij.task.id = status.OId
	ij.parameter.CompareType = status.Name
	switch status.Step {
	case "staticDataJob":
		if err := ij.r.dropTable(diffMetaCollection, resultCollection); err != nil {
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
