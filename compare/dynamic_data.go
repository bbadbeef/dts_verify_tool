package compare

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"sync"
	"time"
)

type dataItem struct {
	val bson.M
	oid interface{}
}

type dynamicDataJob struct {
	assistJob
	srcCh chan *diffItem
	dstCh chan *diffItem

	diffCh     chan *diffItem
	diffBuff   []interface{}
	readers    []*oplogReader
	tsRecorder *timestampRecorder

	srcLocker sync.Mutex
	srcData   map[string]*dataItem
	dstLocker sync.Mutex
	dstData   map[string]*dataItem
}

func (dd *dynamicDataJob) name() string {
	return reflect.TypeOf(dynamicDataJob{}).Name()
}

func (dd *dynamicDataJob) init() error {
	ts, ok := dd.getData("ts").(int64)
	if !ok {
		dd.log.Error("get ts data error")
		return fmt.Errorf("get ts data error")
	}
	dd.diffCh = make(chan *diffItem, buffLengthRatio)
	dd.diffBuff = make([]interface{}, 0, buffLengthRatio)

	dd.tsRecorder = newTimestampRecorder(dd.r, dd.task.id)
	for shard, client := range dd.srcMongodClient {
		dd.readers = append(dd.readers, newOplogReader(shard, client, ts, dd))
		dd.tsRecorder.updateTs(shard, ts)
	}

	if err := dd.updateStep(dd.name()); err != nil {
		dd.log.Errorf("update step error: %s", err.Error())
		return err
	}
	return nil
}

func (dd *dynamicDataJob) do() (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		// 循环遍历diff表，删掉两端数据一致的
		dd.startDiffLoop(ctx)
	}()

	go func() {
		defer wg.Done()
		// 记录oplog里修改的数据到diff表
		dd.recordDiff(ctx)
	}()

	go func() {
		defer wg.Done()
		// 循环读oplog
		dd.readMongodOplog(ctx, cancel)
	}()

	go func() {
		defer wg.Done()
		dd.saveTsLoop(ctx)
	}()
	wg.Wait()
	if dd.error() != nil {
		return false, dd.error()
	}
	return true, nil
}

func (dd *dynamicDataJob) saveTsLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second * 5):
			if err := dd.tsRecorder.saveTs(); err != nil {
				dd.log.Errorf("save ts error: %s", err.Error())
			}
		}
	}
}

func (dd *dynamicDataJob) readMongodOplog(ctx context.Context, cancel context.CancelFunc) {
	var wg sync.WaitGroup
	for _, r := range dd.readers {
		wg.Add(1)
		go func(reader *oplogReader) {
			defer wg.Done()
			if err := reader.read(ctx); err != nil {
				dd.log.Errorf("read oplog error: %s", err.Error())
				dd.setError(err)
				cancel()
				return
			}
		}(r)
	}
	wg.Wait()
}

func (dd *dynamicDataJob) startDiffLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second * 10):
			dd.log.Info("start diff round")
			if err := dd.fetchDiffData(ctx); err != nil {
				dd.log.Errorf("fetch diff data error: %s", err.Error())
				break
			}
			dd.removeSame(ctx)
			dd.log.Info("diff round finish")
		}
	}
}

func (dd *dynamicDataJob) removeSame(ctx context.Context) {
	ch := make(chan interface{}, dd.parameter.DstConcurrency*buffLengthRatio)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dd.doRemove(ctx, ch)
	}()
	for id, data := range dd.srcData {
		if _, ok := dd.dstData[id]; !ok {
			dd.log.Warnf("not found in source: %v", id)
			continue
		}
		if !compareDoc(data.val, dd.dstData[id].val) {
			dd.log.Warnf("not equal between source and destination: %v", id)
			continue
		}
		ch <- data.oid
	}
	close(ch)
	wg.Wait()
}

func (dd *dynamicDataJob) doRemove(ctx context.Context, ch chan interface{}) {
	var wg sync.WaitGroup
	wg.Add(dd.parameter.DstConcurrency)
	for i := 0; i < dd.parameter.DstConcurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case id, ok := <-ch:
					if !ok {
						return
					}
					dd.log.Info("remove from diff: ", id)
					if err := dd.r.removeDiff(id); err != nil {
						dd.log.Errorf("remove from diff error: %s", err.Error())
					}
				}
			}
		}()
	}
	wg.Wait()
}

func (dd *dynamicDataJob) fetchDiffData(ctx context.Context) error {
	dd.srcCh = make(chan *diffItem, dd.parameter.SrcConcurrency*buffLengthRatio)
	dd.dstCh = make(chan *diffItem, dd.parameter.DstConcurrency*buffLengthRatio)
	dd.srcData = make(map[string]*dataItem)
	dd.dstData = make(map[string]*dataItem)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dd.find(ctx)
	}()
	cursor, err := dd.dstClient.Database(dd.parameter.ResultDb).Collection("diff").
		Find(ctx, bson.D{}, &options.FindOptions{})
	if err != nil {
		return err
	}
	for cursor.Next(ctx) {
		var di diffItem
		if err := cursor.Decode(&di); err != nil {
			return err
		}
		dd.srcCh <- &di
		dd.dstCh <- &di
	}
	close(dd.srcCh)
	close(dd.dstCh)
	wg.Wait()
	return nil
}

func (dd *dynamicDataJob) newSrcFindWorker(ctx context.Context, wg *sync.WaitGroup, srcRoutineManager *routineManager) {
	wg.Add(1)
	go func() {
		unit := newRoutineUnit()
		srcRoutineManager.add(unit)
		defer func() {
			srcRoutineManager.remove(unit)
			wg.Done()
			dd.log.Info("a src find routine gone")
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-dd.srcCh:
				if !ok {
					return
				}
				var srcData bson.M
				var err error
				for i := 0; i < retryTimes; i++ {
					srcData, err = findById(ctx, dd.srcClient, item.Ns, item.Id)
					if err != nil {
						dd.log.Errorf("query on source error: %s", err.Error())
						time.Sleep(retryDuration)
						continue
					}
					break
				}
				if err != nil {
					return
				}
				dd.saveSrc(item, srcData)
			}
		}
	}()
}

func (dd *dynamicDataJob) newDstFindWorker(ctx context.Context, wg *sync.WaitGroup, dstRoutineManager *routineManager) {
	wg.Add(1)
	go func() {
		unit := newRoutineUnit()
		dstRoutineManager.add(unit)
		defer func() {
			dstRoutineManager.remove(unit)
			wg.Done()
			dd.log.Info("a dst find routine gone")
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-dd.dstCh:
				if !ok {
					return
				}
				var dstData bson.M
				var err error
				for i := 0; i < retryTimes; i++ {
					dstData, err = findById(ctx, dd.dstClient, item.Ns, item.Id)
					if err != nil {
						dd.log.Errorf("query on destination error: %s", err.Error())
						time.Sleep(retryDuration)
						continue
					}
					break
				}
				if err != nil {
					return
				}
				dd.saveDst(item, dstData)
			}
		}
	}()
}

func (dd *dynamicDataJob) find(ctx context.Context) {
	var wg sync.WaitGroup
	srcRoutineManager := &routineManager{}
	dstRoutineManager := &routineManager{}
	for i := 0; i < dd.parameter.SrcConcurrency; i++ {
		dd.newSrcFindWorker(ctx, &wg, srcRoutineManager)
	}
	for i := 0; i < dd.parameter.DstConcurrency; i++ {
		dd.newDstFindWorker(ctx, &wg, dstRoutineManager)
	}
	go func() {
		for {
			time.Sleep(time.Second * 5)
			if !dd.parameter.dirty {
				continue
			}
			{
				current := srcRoutineManager.len()
				desired := dd.parameter.SrcConcurrency
				if current > desired {
					srcRoutineManager.destroyN(current - desired)
				} else if desired > current {
					for i := 0; i < desired-current; i++ {
						dd.newSrcFindWorker(ctx, &wg, srcRoutineManager)
					}
				}
			}

			{
				current := dstRoutineManager.len()
				desired := dd.parameter.DstConcurrency
				if current > desired {
					dstRoutineManager.destroyN(current - desired)
				} else if desired > current {
					for i := 0; i < desired-current; i++ {
						dd.newDstFindWorker(ctx, &wg, dstRoutineManager)
					}
				}
			}
			dd.parameter.dirty = false
		}
	}()
	wg.Wait()
}

func (dd *dynamicDataJob) saveSrc(item *diffItem, val bson.M) {
	dd.srcLocker.Lock()
	dd.srcData[fmt.Sprintf("%v", item.OId)] = &dataItem{
		val: val,
		oid: item.OId,
	}
	dd.srcLocker.Unlock()
}

func (dd *dynamicDataJob) saveDst(item *diffItem, val bson.M) {
	dd.dstLocker.Lock()
	dd.dstData[fmt.Sprintf("%v", item.OId)] = &dataItem{
		val: val,
		oid: item.OId,
	}
	dd.dstLocker.Unlock()
}

func (dd *dynamicDataJob) recordDiff(ctx context.Context) {
	start := time.Now()
	var ts int64
	doRecord := func(t int64) {
		if len(dd.diffBuff) != 0 {
			if err := dd.r.saveDiff(dd.diffBuff); err != nil {
				dd.log.Errorf("save diff error: %s", err.Error())
				return
			}
		}
		for _, item := range dd.diffBuff {
			it, _ := item.(*diffItem)
			if it == nil {
				continue
			}
			dd.tsRecorder.updateTs(it.shard, it.Ts)
		}
		dd.diffBuff = dd.diffBuff[:0:0]
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second * 10):
			doRecord(ts)
		case item := <-dd.diffCh:
			dd.diffBuff = append(dd.diffBuff, item)
			if ts < item.Ts {
				ts = item.Ts
			}
			if len(dd.diffBuff) >= buffLengthRatio || time.Now().Sub(start) > time.Second*10 {
				if len(dd.diffBuff) < buffLengthRatio {
					start = time.Now()
				}
				doRecord(ts)
			}
		}
	}
}

type oplogReader struct {
	shard  int
	c      *mongo.Client
	ts     int64
	diffCh chan *diffItem
	log    *logrus.Logger
	job    *dynamicDataJob
}

func newOplogReader(shard int, c *mongo.Client, ts int64, job *dynamicDataJob) *oplogReader {
	return &oplogReader{
		shard:  shard,
		c:      c,
		ts:     ts,
		diffCh: job.diffCh,
		log:    job.log,
		job:    job,
	}
}

func (or *oplogReader) read(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second * 10):
			if err := or.readOplog(ctx); err != nil {
				or.log.Errorf("read oplog error: %s", err.Error())
				return err
			}
		}
	}
}

func (or *oplogReader) readOplog(ctx context.Context) error {
	or.log.Infof("new round read oplog, shard: %d, ts: %d", or.shard, or.ts)
	cursor, err := or.c.Database("local").Collection("oplog.rs").Find(ctx,
		bson.D{{"op", bson.M{"$in": []string{"i", "u", "d"}}},
			{"ts", bson.M{"$gte": primitive.Timestamp{T: uint32(or.ts), I: 0}}}},
		&options.FindOptions{Sort: bson.M{"ts": 1}})
	if err != nil {
		or.log.Errorf("read oplog error: %s", err.Error())
		return err
	}
	for cursor.Next(ctx) {
		var od oplogDoc
		if err := cursor.Decode(&od); err != nil {
			or.log.Errorf("decode oplog error: %s", err.Error())
			return err
		}
		if !filterOplog(&od, or.job.parameter.SpecifiedDb, or.job.parameter.SpecifiedNs) {
			continue
		}
		di := parseOplog(&od)
		if di == nil {
			or.log.Errorf("parse oplog error: %v", od)
			continue
		}
		// fmt.Println(od.Ns, od.Op, od.Ts.Unix(), od.O2, od.O)
		di.shard = or.shard
		or.send(di)
	}
	or.log.Infof("read to oplog end, will start a new round, shard: %d", or.shard)
	or.ts = time.Now().Unix() - 1
	or.job.tsRecorder.updateTs(or.shard, or.ts)
	return nil
}

func (or *oplogReader) send(di *diffItem) {
	or.diffCh <- di
}

type timestampRecorder struct {
	sync.Mutex
	ts     map[int]int64
	r      *record
	taskId string
}

func newTimestampRecorder(r *record, taskId string) *timestampRecorder {
	return &timestampRecorder{
		ts:     make(map[int]int64),
		r:      r,
		taskId: taskId,
	}
}

func (tr *timestampRecorder) updateTs(shard int, ts int64) {
	tr.Lock()
	if tr.ts[shard] < ts {
		tr.ts[shard] = ts
	}
	tr.Unlock()
}

func (tr *timestampRecorder) getSmallestTs() int64 {
	var ts int64
	tr.Lock()
	for _, t := range tr.ts {
		if ts == 0 {
			ts = t
		}
		if t < ts {
			ts = t
		}
	}
	tr.Unlock()
	return ts
}

func (tr *timestampRecorder) saveTs() error {
	ts := tr.getSmallestTs()
	return tr.r.updateStatus(tr.taskId, map[string]interface{}{
		"ts": ts,
	})
}
