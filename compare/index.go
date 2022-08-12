package compare

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"reflect"
	"strings"
	"sync"
)

type indexJob struct {
	assistJob
	ns map[string][]string

	srcIndex map[string]map[string]map[string]interface{}
	dstIndex map[string]map[string]map[string]interface{}
}

func (ij *indexJob) name() string {
	//return reflect.TypeOf(*ij).Name()
	return "索引"
}

func (ij *indexJob) init() error {
	var ok bool
	ij.ns, ok = ij.getData("srcNsCleaned").(map[string][]string)
	if !ok || ij.ns == nil {
		ij.log.Error("get namespace data error")
		return fmt.Errorf("get namespace data error")
	}
	ij.srcIndex = make(map[string]map[string]map[string]interface{}, 0)
	ij.dstIndex = make(map[string]map[string]map[string]interface{}, 0)
	return nil
}

func (ij *indexJob) do() (bool, error) {
	ij.log.Info("begin index compare")
	ctx, cancel := context.WithCancel(ij.ctx)
	defer cancel()
	srcCh := make(chan *nss, ij.parameter.SrcConcurrency)
	dstCh := make(chan *nss, ij.parameter.DstConcurrency)
	var wg sync.WaitGroup
	var srcMutex sync.Mutex
	var dstMutex sync.Mutex
	for i := 0; i < ij.parameter.SrcConcurrency; i++ {
		go ij.indexGetter(ctx, cancel, ij.srcClient, srcCh, ij.srcIndex, &wg, &srcMutex)
	}
	for i := 0; i < ij.parameter.DstConcurrency; i++ {
		go ij.indexGetter(ctx, cancel, ij.dstClient, dstCh, ij.dstIndex, &wg, &dstMutex)
	}
	var sendWg sync.WaitGroup
	sendWg.Add(2)
	go func() {
		defer sendWg.Done()
		for db, coll := range ij.ns {
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
		for db, coll := range ij.ns {
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

	if ij.error() != nil {
		return false, ij.error()
	}

	ij.log.Info("index compare finish")

	result := &JobResult{
		Task:      ij.subName,
		Step:      ij.name(),
		Identical: true,
	}
	if !reflect.DeepEqual(ij.srcIndex, ij.dstIndex) {
		diffs := diffIndex(ij.srcIndex, ij.dstIndex)
		if len(diffs) != 0 {
			result.Identical = false
			result.Diff = diffs
			ij.notifyDiff(Index, Update, diffs)
		}
	}
	if err := ij.r.saveResult(result); err != nil {
		ij.log.Errorf("save result error: %s", err.Error())
	}
	return true, nil
}

func (ij *indexJob) indexGetter(ctx context.Context, cancel context.CancelFunc, c *mongo.Client,
	ch chan *nss, res map[string]map[string]map[string]interface{}, wg *sync.WaitGroup, m *sync.Mutex) {
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
			indexes, err := getIndexes(ctx, c, ns.db, ns.collection)
			if err != nil {
				if strings.Contains(err.Error(), "CommandNotSupportedOnView") {
					ij.log.Infof("%s.%s is a view, skip get index", ns.db, ns.collection)
					continue
				}
				ij.log.Errorf("get index error, ns: %s.%s, error: %s", ns.db, ns.collection, err.Error())
				ij.setError(err)
				cancel()
				return
			}
			m.Lock()
			res[ns.db+"."+ns.collection] = indexes
			m.Unlock()
		}
	}
}
