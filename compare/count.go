package compare

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"sync"
)

type nss struct {
	db         string
	collection string
}

type countCompareJob struct {
	assistJob
	srcCount     map[string]int64
	dstCount     map[string]int64
	srcNsCleaned map[string][]string
	dstNsCleaned map[string][]string
}

func (cc *countCompareJob) name() string {
	//return reflect.TypeOf(*cc).Name()
	return "行数"
}

func (cc *countCompareJob) init() error {
	cc.srcCount = make(map[string]int64)
	cc.dstCount = make(map[string]int64)
	var ok bool
	cc.srcNsCleaned, ok = cc.getData("srcNsCleaned").(map[string][]string)
	if !ok || cc.srcNsCleaned == nil {
		cc.log.Error("get source namespace data error")
		return fmt.Errorf("get source namespace data error")
	}
	cc.dstNsCleaned, ok = cc.getData("dstNsCleaned").(map[string][]string)
	if !ok || cc.dstNsCleaned == nil {
		cc.log.Error("get destination namespace data error")
		return fmt.Errorf("get destination namespace data error")
	}

	if err := cc.updateStep(cc.name()); err != nil {
		return err
	}
	return nil
}

func (cc *countCompareJob) getCount(ctx context.Context, c *mongo.Collection) (int64, error) {
	subCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if cc.tName == "data_count" {
		return c.CountDocuments(subCtx, bson.D{}, options.Count().SetMaxTime(timeout))
	}
	return c.EstimatedDocumentCount(subCtx, options.EstimatedDocumentCount().SetMaxTime(timeout))
}

func (cc *countCompareJob) do() (bool, error) {
	cc.log.Info("begin count compare")
	ctx, cancel := context.WithCancel(cc.ctx)
	defer cancel()
	srcCh := make(chan *nss, cc.parameter.SrcConcurrency)
	dstCh := make(chan *nss, cc.parameter.DstConcurrency)
	var wg sync.WaitGroup
	var srcMutex sync.Mutex
	var dstMutex sync.Mutex
	for i := 0; i < cc.parameter.SrcConcurrency; i++ {
		go cc.countGetter(ctx, cancel, cc.srcClient, srcCh, cc.srcCount, &wg, &srcMutex)
	}
	for i := 0; i < cc.parameter.DstConcurrency; i++ {
		go cc.countGetter(ctx, cancel, cc.dstClient, dstCh, cc.dstCount, &wg, &dstMutex)
	}
	var sendWg sync.WaitGroup
	sendWg.Add(2)
	go func() {
		defer sendWg.Done()
		for db, coll := range cc.srcNsCleaned {
			for _, c := range coll {
				select {
				case <-ctx.Done():
					return
				case srcCh <- &nss{db: db, collection: c}:
				}

			}
		}
	}()
	go func() {
		defer sendWg.Done()
		for db, coll := range cc.dstNsCleaned {
			for _, c := range coll {
				select {
				case <-ctx.Done():
					return
				case dstCh <- &nss{db: db, collection: c}:
				}

			}
		}
	}()
	sendWg.Wait()
	close(srcCh)
	close(dstCh)
	wg.Wait()

	if cc.error() != nil {
		return false, cc.error()
	}

	cc.log.Info("count compare finish")

	if cc.tName == "data" && (cc.parameter.CompareExtra == 0 || cc.parameter.CompareExtra&Count != 0) {
		result := &JobResult{
			Task:      cc.tName,
			Step:      cc.name(),
			Identical: true,
		}
		if !reflect.DeepEqual(cc.srcCount, cc.dstCount) {
			diffs := diffCount(cc.srcCount, cc.dstCount)
			if len(diffs) != 0 {
				result.Identical = false
				result.Diff = diffs
				cc.notifyDiff(Count, Update, diffs)
			}
		}
		if err := cc.r.saveResult(result); err != nil {
			cc.log.Errorf("save result error: %s", err.Error())
		}
	}
	cc.setData("srcCount", cc.srcCount)
	cc.setData("dstCount", cc.dstCount)
	return true, nil
}

func (cc *countCompareJob) countGetter(ctx context.Context, cancel context.CancelFunc, c *mongo.Client,
	ch chan *nss, res map[string]int64, wg *sync.WaitGroup, m *sync.Mutex) {
	wg.Add(1)
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case ns, ok := <-ch:
			if !ok {
				return
			}
			count, err := cc.getCount(ctx, c.Database(ns.db).Collection(ns.collection))
			if err != nil {
				cc.setError(err)
				cancel()
				return
			}
			m.Lock()
			res[ns.db+"."+ns.collection] = count
			m.Unlock()
		}
	}
}
