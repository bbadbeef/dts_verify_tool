package compare

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"reflect"
	"sync"
)

type nsJob struct {
	assistJob
	srcNs map[string][]string
	dstNs map[string][]string
}

func (nj *nsJob) name() string {
	//return reflect.TypeOf(*nj).Name()
	return "获取库表"
}

func (nj *nsJob) init() error {
	nj.srcNs = make(map[string][]string)
	nj.dstNs = make(map[string][]string)

	if err := nj.updateStep(nj.name()); err != nil {
		return err
	}
	return nil
}

func (nj *nsJob) do() (bool, error) {
	nj.log.Info("start get namespace")
	b, err := nj.listDb()
	if err != nil || !b {
		return false, err
	}

	b, err = nj.listCollection()
	if err != nil || !b {
		return false, err
	}

	nj.setData("srcNs", nj.srcNs)
	nj.setData("dstNs", nj.dstNs)
	nj.log.Info("get namespace finish")
	nj.log.Info("source ns: ", nj.srcNs)
	nj.log.Info("destination ns: ", nj.dstNs)
	return true, nil
}

func (nj *nsJob) listDb() (bool, error) {
	nj.log.Info("start list db")
	var wg sync.WaitGroup
	wg.Add(2)
	var srcErr, dstErr error
	dbGetter := func(c *mongo.Client, res map[string][]string, e *error) {
		defer wg.Done()
		dbs, err := showDbs(c)
		if err != nil {
			*e = err
			return
		}
		for _, db := range dbs {
			if db == "TencetDTSData" || db == nj.task.p.ResultDb {
				continue
			}
			res[db] = nil
		}
	}
	go dbGetter(nj.srcClient, nj.srcNs, &srcErr)
	go dbGetter(nj.dstClient, nj.dstNs, &dstErr)
	wg.Wait()

	if srcErr != nil {
		return false, srcErr
	}
	if dstErr != nil {
		return false, dstErr
	}

	nj.log.Info("list db finish")
	return true, nil
}

func (nj *nsJob) listCollection() (bool, error) {
	nj.log.Info("start list collection")
	if len(nj.srcNs) == 0 && len(nj.dstNs) == 0 {
		return true, nil
	}

	var wg sync.WaitGroup
	wg.Add(nj.parameter.SrcConcurrency + nj.parameter.DstConcurrency)
	ctx, cancel := context.WithCancel(nj.ctx)
	defer cancel()
	var e error
	srcCh := make(chan string, nj.parameter.SrcConcurrency)
	dstCh := make(chan string, nj.parameter.DstConcurrency)
	collGetter := func(c *mongo.Client, ch chan string, ns map[string][]string, m *sync.Mutex) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case db, ok := <-ch:
				if !ok {
					return
				}
				cs, err := showCollections(c, db)
				if err != nil {
					cancel()
					e = err
					return
				}
				m.Lock()
				ns[db] = cs
				m.Unlock()
			}
		}
	}
	var srcMutex sync.Mutex
	var dstMutex sync.Mutex
	for i := 0; i < nj.parameter.SrcConcurrency; i++ {
		go collGetter(nj.srcClient, srcCh, nj.srcNs, &srcMutex)
	}
	for i := 0; i < nj.parameter.DstConcurrency; i++ {
		go collGetter(nj.dstClient, dstCh, nj.dstNs, &dstMutex)
	}
	var sendWg sync.WaitGroup
	sendWg.Add(2)
	go func() {
		defer sendWg.Done()
		for db, _ := range nj.srcNs {
			select {
			case <-ctx.Done():
				return
			case srcCh <- db:
			}
		}
	}()
	go func() {
		defer sendWg.Done()
		for db, _ := range nj.dstNs {
			select {
			case <-ctx.Done():
				return
			case dstCh <- db:
			}
		}
	}()
	sendWg.Wait()
	close(srcCh)
	close(dstCh)
	wg.Wait()

	if e != nil {
		return false, e
	}

	nj.log.Info("list collection finish")
	return true, nil
}

type nsFilterBaseJob struct {
	assistJob
	srcNs        map[string][]string
	dstNs        map[string][]string
	srcNsCleaned map[string][]string
	dstNsCleaned map[string][]string
	f            func(mc *mongo.Client, before, after map[string][]string)
	step         string
}

func (nfj *nsFilterBaseJob) name() string {
	//return nfj.step
	return "库表信息"
}

func (nfj *nsFilterBaseJob) init() error {
	nfj.srcNsCleaned = make(map[string][]string)
	nfj.dstNsCleaned = make(map[string][]string)
	var ok bool
	nfj.srcNs, ok = nfj.getData("srcNs").(map[string][]string)
	if !ok || nfj.srcNs == nil {
		nfj.log.Error("get source namespace data error")
		return fmt.Errorf("get source namespace data error")
	}
	nfj.dstNs, ok = nfj.getData("dstNs").(map[string][]string)
	if !ok || nfj.dstNs == nil {
		nfj.log.Error("get destination namespace data error")
		return fmt.Errorf("get destination namespace data error")
	}

	if err := nfj.updateStep(nfj.name()); err != nil {
		return err
	}
	return nil
}

func (nfj *nsFilterBaseJob) polymorphism() {
	nfj.tName = "data"
	nfj.step = reflect.TypeOf(*nfj).Name()
	nfj.f = func(mc *mongo.Client, before, after map[string][]string) {
		for db, coll := range before {
			if inList(db, systemDb) {
				continue
			}
			for _, c := range coll {
				if inListRegex(c, systemCollection) {
					continue
				}
				if isView(mc, db, c) {
					nfj.log.Warnf("%s.%s is view, skip it", db, c)
					continue
				}
				after[db] = append(after[db], c)
			}
		}
	}
}

func (nfj *nsFilterBaseJob) do() (bool, error) {
	nfj.f(nfj.srcClient, nfj.srcNs, nfj.srcNsCleaned)
	nfj.f(nfj.dstClient, nfj.dstNs, nfj.dstNsCleaned)

	nfj.log.Info("source cleaned ns: ", nfj.srcNsCleaned)
	nfj.log.Info("destination cleaned ns: ", nfj.dstNsCleaned)

	if nfj.tName == "data" && (nfj.parameter.CompareExtra == 0 || nfj.parameter.CompareExtra&Namespace != 0) {
		diff := diffNs(nfj.srcNsCleaned, nfj.dstNsCleaned)
		if err := nfj.r.saveResult(&JobResult{
			Task:      nfj.tName,
			Step:      nfj.name(),
			Identical: len(diff) == 0,
			Diff:      diff,
		}); err != nil {
			nfj.log.Errorf("save result error: %s", err.Error())
		}
		nfj.notifyDiff(Namespace, Update, diff)
	}
	nfj.setData("srcNsCleaned", nfj.srcNsCleaned)
	nfj.setData("dstNsCleaned", nfj.dstNsCleaned)
	return true, nil
}

type nsFilterIndexJob struct {
	nsFilterBaseJob
}

func (na *nsFilterIndexJob) polymorphism() {
	na.nsFilterBaseJob.polymorphism()
	na.tName = "index"
}

type nsFilterAccountJob struct {
	nsFilterBaseJob
}

func (na *nsFilterAccountJob) name() string {
	// return na.step
	return "库表信息"
}

func (na *nsFilterAccountJob) polymorphism() {
	na.tName = "account"
	na.step = reflect.TypeOf(nsFilterAccountJob{}).Name()
	na.f = func(mc *mongo.Client, before, after map[string][]string) {
		for db, coll := range before {
			for _, c := range coll {
				if c == "system.users" {
					after[db] = append(after[db], c)
				}
			}
		}
	}
}

type nsFilterShardKeyJob struct {
	nsFilterBaseJob
}

func (ns *nsFilterShardKeyJob) name() string {
	//return ns.step
	return "库表信息"
}

func (ns *nsFilterShardKeyJob) polymorphism() {
	ns.tName = "shard_key"
	ns.step = reflect.TypeOf(nsFilterShardKeyJob{}).Name()
	ns.f = func(mc *mongo.Client, before, after map[string][]string) {
		after["config"] = []string{"collections"}
	}
}

type nsFilterTagJob struct {
	nsFilterBaseJob
}

func (nt *nsFilterTagJob) name() string {
	//return nt.step
	return "库表信息"
}

func (nt *nsFilterTagJob) polymorphism() {
	nt.tName = "tag"
	nt.step = reflect.TypeOf(nsFilterTagJob{}).Name()
	nt.f = func(mc *mongo.Client, before, after map[string][]string) {
		after["config"] = []string{"tags"}
	}
}

type nsFilterJavascriptJob struct {
	nsFilterBaseJob
}

func (nj *nsFilterJavascriptJob) name() string {
	//return nj.step
	return "库表信息"
}

func (nj *nsFilterJavascriptJob) polymorphism() {
	nj.tName = "javascript"
	nj.step = reflect.TypeOf(nsFilterJavascriptJob{}).Name()
	nj.f = func(mc *mongo.Client, before, after map[string][]string) {
		for db, coll := range before {
			for _, c := range coll {
				if c == "system.js" {
					after[db] = append(after[db], c)
				}
			}
		}
	}
}

type nsFilterSpecifiedJob struct {
	nsFilterBaseJob
}

func (ns *nsFilterSpecifiedJob) name() string {
	//return ns.step
	return "库表信息"
}

func (ns *nsFilterSpecifiedJob) polymorphism() {
	ns.tName = "data"
	ns.step = reflect.TypeOf(nsFilterSpecifiedJob{}).Name()
	ns.f = func(mc *mongo.Client, before, after map[string][]string) {
		for db, coll := range before {
			if len(ns.parameter.SpecifiedDb) != 0 && !inList(db, ns.parameter.SpecifiedDb) {
				continue
			}
			if inList(db, systemDb) {
				continue
			}
			for _, c := range coll {
				if len(ns.parameter.SpecifiedNs) != 0 &&
					!inList(fmt.Sprintf("%s.%s", db, c), ns.parameter.SpecifiedNs) {
					continue
				}
				if isView(mc, db, c) {
					ns.log.Warnf("%s.%s is view, skip it", db, c)
					continue
				}
				if inListRegex(c, systemCollection) {
					continue
				}
				after[db] = append(after[db], c)
			}
		}
	}
}
