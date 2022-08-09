package compare

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
)

type dataItem struct {
	val bson.M
	oid interface{}
	ns  string
	id  interface{}
}

type diffOid struct {
	oid interface{}
	act int
}

type dynamicDataJob struct {
	assistJob
	srcCh chan *DiffItem
	dstCh chan *DiffItem
	ch    chan *DiffItem

	diffCh     chan *DiffItem // oplog读取的原始数据
	diffBuff   []interface{}
	readers    []*oplogReader
	tsRecorder *timestampRecorder

	endFlag   bool
	srcLocker sync.Mutex
	srcData   map[string]*dataItem
	dstLocker sync.Mutex
	dstData   map[string]*dataItem
}

func (dd *dynamicDataJob) name() string {
	//return reflect.TypeOf(dynamicDataJob{}).Name()
	return "增量"
}

func (dd *dynamicDataJob) init() error {
	ts, ok := dd.getData("ts").(int64)
	if !ok {
		dd.log.Error("get ts data error")
		return fmt.Errorf("get ts data error")
	}
	dd.diffCh = make(chan *DiffItem, buffLengthRatio)
	dd.diffBuff = make([]interface{}, 0, buffLengthRatio)

	dd.tsRecorder = newTimestampRecorder(dd.r, dd.task.id)
	for shard, client := range dd.srcMongodClient {
		dd.readers = append(dd.readers, newOplogReader(shard, client, ts, dd))
		dd.tsRecorder.updateTs(shard, ts)
	}

	if err := dd.updateStepSimple(dd.name()); err != nil {
		dd.log.Errorf("update step error: %s", err.Error())
		return err
	}
	return nil
}

func (dd *dynamicDataJob) do() (bool, error) {
	ctx, cancel := context.WithCancel(dd.ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		time.Sleep(time.Second * 10)
		// 循环遍历diff表，删掉两端数据一致的
		dd.startDiffLoop1(ctx)
	}()

	go func() {
		defer wg.Done()
		// 记录不一致的数据到diff表
		dd.recordDiff(ctx)
	}()

	go func() {
		defer wg.Done()
		// 循环读oplog
		dd.readMongodOplog(ctx, cancel)
		dd.endFlag = true
		close(dd.diffCh)
	}()

	go func() {
		dd.saveTsLoop(ctx)
	}()

	wg.Wait()
	dd.log.Info("dynamic data compare finish")
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
		case <-time.After(time.Second * 5):
			dd.log.Info("start diff round")
			willFinish := dd.endFlag
			if err := dd.fetchDiffData(ctx); err != nil {
				dd.log.Errorf("fetch diff data error: %s", err.Error())
				break
			}
			dd.removeSame(ctx)
			dd.log.Info("diff round finish")
			if willFinish {
				return
			}
		}
	}
}

func (dd *dynamicDataJob) startDiffLoop1(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second * 5):
			dd.log.Info("start diff round")
			willFinish := dd.endFlag
			if err := dd.fetchDiffData1(ctx); err != nil {
				dd.log.Errorf("fetch diff data error: %s", err.Error())
				break
			}
			dd.log.Info("diff round finish")
			if willFinish {
				return
			}
		}
	}
}

func (dd *dynamicDataJob) fetchDiffData1(ctx context.Context) error {
	concurrency := dd.parameter.SrcConcurrency
	if concurrency > dd.parameter.DstConcurrency {
		concurrency = dd.parameter.DstConcurrency
	}
	dd.ch = make(chan *DiffItem, concurrency*buffLengthRatio)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dd.find1(ctx, concurrency)
	}()
	cursor, err := dd.dstClient.Database(dd.parameter.ResultDb).Collection(dd.r.diffCollection()).
		Find(ctx, bson.D{}, &options.FindOptions{})
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())
	for cursor.Next(ctx) {
		var di DiffItem
		if err := cursor.Decode(&di); err != nil {
			return err
		}
		dd.ch <- &di
	}
	close(dd.ch)
	wg.Wait()
	return nil
}

func (dd *dynamicDataJob) find1(ctx context.Context, concurrency int) {
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		dd.newFindWorker(ctx, &wg)
	}
	wg.Wait()
}

func (dd *dynamicDataJob) newFindWorker(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-dd.ch:
				if !ok {
					return
				}
				srcData, err := findByIdWithRetry(ctx, dd.srcClient, item.Ns, item.Id)
				if err != nil {
					dd.log.Errorf("query on source error: %s", err.Error())
					return
				}
				dstData, err := findByIdWithRetry(ctx, dd.dstClient, item.Ns, item.Id)
				if err != nil {
					dd.log.Errorf("query on destination error: %s", err.Error())
					return
				}
				if compareDoc(srcData, dstData) {
					if err := dd.r.removeDiff(item.OId); err != nil {
						dd.log.Errorf("remove from diff error: %s", err.Error())
					}
				} else {
					if err := dd.r.confirmDiff(item.OId); err != nil {
						dd.log.Errorf("confirm diff error: %s", err.Error())
					}
				}
			}
		}
	}()
}

func (dd *dynamicDataJob) removeSame(ctx context.Context) {
	ch := make(chan *diffOid, dd.parameter.DstConcurrency*buffLengthRatio)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dd.doRemove(ctx, ch)
	}()
	for id, data := range dd.srcData {
		act := Update
		if _, ok := dd.dstData[id]; !ok {
			dd.log.Warnf("not found in destination: %v", id)
		} else if !compareDoc(data.val, dd.dstData[id].val) {
			dd.log.Warnf("not equal between source and destination: %v", id)
		} else {
			act = Undo
		}
		ch <- &diffOid{
			oid: data.oid,
			act: act,
		}
		dd.notifyDiff(Data, act, []*MetaDiffItem{
			{
				Ns:    data.ns,
				SrcId: data.id,
				DstId: func() interface{} {
					if _, ok := dd.dstData[id]; !ok {
						return nil
					}
					if len(dd.dstData[id].val) == 0 {
						return nil
					}
					return data.id
				}(),
				SrcVal: data.val,
				DstVal: func() interface{} {
					if _, ok := dd.dstData[id]; !ok {
						return nil
					}
					if len(dd.dstData[id].val) == 0 {
						return nil
					}
					return dd.dstData[id].val
				}(),
			},
		})
	}
	close(ch)
	wg.Wait()
}

func (dd *dynamicDataJob) doRemove(ctx context.Context, ch chan *diffOid) {
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
					if id.act == Undo {
						dd.log.Info("remove from diff: ", id.oid)
						if err := dd.r.removeDiff(id.oid); err != nil {
							dd.log.Errorf("remove from diff error: %s", err.Error())
						}
					} else {
						if err := dd.r.confirmDiff(id.oid); err != nil {
							dd.log.Errorf("confirm diff error: %s", err.Error())
						}
					}
				}
			}
		}()
	}
	wg.Wait()
}

func (dd *dynamicDataJob) fetchDiffData(ctx context.Context) error {
	dd.srcCh = make(chan *DiffItem, dd.parameter.SrcConcurrency*buffLengthRatio)
	dd.dstCh = make(chan *DiffItem, dd.parameter.DstConcurrency*buffLengthRatio)
	dd.srcData = make(map[string]*dataItem)
	dd.dstData = make(map[string]*dataItem)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dd.find(ctx)
	}()
	cursor, err := dd.dstClient.Database(dd.parameter.ResultDb).Collection(dd.r.diffCollection()).
		Find(ctx, bson.D{}, &options.FindOptions{})
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())
	for cursor.Next(ctx) {
		var di DiffItem
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
			// dd.log.Info("a src find routine gone")
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-dd.srcCh:
				if !ok {
					return
				}
				srcData, err := findByIdWithRetry(ctx, dd.srcClient, item.Ns, item.Id)
				if err != nil {
					dd.log.Errorf("query on source error: %s", err.Error())
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
			// dd.log.Info("a dst find routine gone")
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-dd.dstCh:
				if !ok {
					return
				}
				dstData, err := findByIdWithRetry(ctx, dd.dstClient, item.Ns, item.Id)
				if err != nil {
					dd.log.Errorf("query on destination error: %s", err.Error())
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

func (dd *dynamicDataJob) saveSrc(item *DiffItem, val bson.M) {
	dd.srcLocker.Lock()
	dd.srcData[fmt.Sprintf("%v", item.OId)] = &dataItem{
		val: val,
		oid: item.OId,
		ns:  item.Ns,
		id:  item.Id,
	}
	dd.srcLocker.Unlock()
}

func (dd *dynamicDataJob) saveDst(item *DiffItem, val bson.M) {
	dd.dstLocker.Lock()
	dd.dstData[fmt.Sprintf("%v", item.OId)] = &dataItem{
		val: val,
		oid: item.OId,
		ns:  item.Ns,
		id:  item.Id,
	}
	dd.dstLocker.Unlock()
}

//func (dd *dynamicDataJob) filterDiff(ctx context.Context) {
//	for {
//		select {
//		case <-ctx.Done():
//			return
//		case item, ok := <-dd.diffCh:
//			if !ok {
//				return
//			}
//			srcDataItem, err := dd.findByIdWithRetry(ctx, dd.srcClient, item.Ns, item.Id)
//			if err != nil {
//				return
//			}
//			dstDataItem, err := dd.findByIdWithRetry(ctx, dd.dstClient, item.Ns, item.Id)
//			if err != nil {
//				return
//			}
//			if compareDoc(srcDataItem, dstDataItem) {
//				continue
//			}
//			dd.diffCleanCh <- item
//		}
//	}
//}

func (dd *dynamicDataJob) recordDiff(ctx context.Context) {
	start := time.Now()
	doRecord := func() {
		if len(dd.diffBuff) != 0 {
			if err := dd.r.saveDiff(dd.diffBuff); err != nil {
				dd.log.Errorf("save diff error: %s", err.Error())
				return
			}
			for _, item := range dd.diffBuff {
				it := item.(*DiffItem)
				dd.tsRecorder.updateTs(it.shard, it.Ts)
			}
		}
		dd.diffBuff = dd.diffBuff[:0:0]
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second * 5):
			doRecord()
		case item, ok := <-dd.diffCh:
			if !ok {
				return
			}
			dd.diffBuff = append(dd.diffBuff, item)
			if len(dd.diffBuff) >= buffLengthRatio || time.Now().Sub(start) > time.Second*5 {
				if len(dd.diffBuff) < buffLengthRatio {
					start = time.Now()
				}
				doRecord()
			}
		}
	}
}

type oplogReader struct {
	shard       int
	c           *mongo.Client
	ts          int64
	nextRoundTs int64
	round       int
	diffCh      chan *DiffItem
	log         *logrus.Logger
	job         *dynamicDataJob
}

func newOplogReader(shard int, c *mongo.Client, ts int64, job *dynamicDataJob) *oplogReader {
	return &oplogReader{
		shard:  shard,
		c:      c,
		ts:     ts,
		round:  0,
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
			if b, err := or.readOplog(ctx); err != nil {
				or.log.Errorf("read oplog error: %s", err.Error())
				return err
			} else if b {
				return nil
			}
		}
	}
}

func (or *oplogReader) readOplog(ctx context.Context) (bool, error) {
	or.log.Infof("new round read oplog, round: %d, shard: %d, ts: %d", or.round, or.shard, or.ts)
	or.nextRoundTs = time.Now().Unix()
	or.job.tsRecorder.updateTs(or.shard, or.nextRoundTs)
	if err := or.job.tsRecorder.saveTs(); err != nil {
		or.log.Error("save ts error: ", err.Error())
	}

	cursor, err := or.c.Database("local").Collection("oplog.rs").Find(ctx,
		bson.D{{"op", bson.M{"$in": []string{"i", "u", "d"}}},
			{"ts", bson.M{"$gte": primitive.Timestamp{T: uint32(or.ts), I: 0}}}},
		&options.FindOptions{})
	if err != nil {
		or.log.Errorf("read oplog error: %s", err.Error())
		return false, err
	}
	defer cursor.Close(context.Background())
	for cursor.Next(ctx) {
		var od oplogDoc
		if err := cursor.Decode(&od); err != nil {
			or.log.Errorf("decode oplog error: %s", err.Error())
			return false, err
		}
		// or.job.tsRecorder.updateTs(or.shard, od.Ts.Unix())
		or.ts = od.Ts.Unix()
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
	or.log.Infof("read to oplog end, round: %d, shard: %d, ts: %d", or.round, or.shard, or.ts)
	or.round++
	if or.finish() {
		or.log.Info("task will finish")
		return true, nil
	}
	or.log.Infof("will start a new round: round: %d, shard: %d, ts: %d", or.round, or.shard, or.ts)
	return false, nil
}

func (or *oplogReader) finish() bool {
	return or.job.parameter.FiniteFunc()
	//if or.round >= 6 {
	//	return true
	//}
	//if time.Now().Unix()-or.ts < 30 {
	//	return true
	//}
	//return false
}

func (or *oplogReader) send(di *DiffItem) {
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
