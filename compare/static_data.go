package compare

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math"
	"mongo_compare/utils"
	"reflect"
	"sync"
	"time"
)

type accountDataJob struct {
	staticDataJob
}

func (adj *accountDataJob) polymorphism() {
	adj.helper = &staticDataAccountHelper{}
	adj.helper.setRecord(adj.r)
	adj.helper.setCb(adj)
}

func (adj *accountDataJob) name() string {
	return reflect.TypeOf(accountDataJob{}).Name()
}

type shardKeyDataJob struct {
	staticDataJob
}

func (sdj *shardKeyDataJob) polymorphism() {
	sdj.helper = &staticDataShardHelper{}
	sdj.helper.setRecord(sdj.r)
	sdj.helper.setCb(sdj)
}
func (sdj *shardKeyDataJob) name() string {
	return reflect.TypeOf(shardKeyDataJob{}).Name()
}

type tagDataJob struct {
	staticDataJob
}

func (tdj *tagDataJob) name() string {
	return reflect.TypeOf(tagDataJob{}).Name()
}

func (tdj *tagDataJob) polymorphism() {
	tdj.helper = &staticDataMetaHelper{}
	tdj.helper.setRecord(tdj.r)
	tdj.helper.setCb(tdj)
}

type javascriptDataJob struct {
	staticDataJob
}

func (jdj *javascriptDataJob) name() string {
	return reflect.TypeOf(javascriptDataJob{}).Name()
}

func (jdj *javascriptDataJob) polymorphism() {
	jdj.helper = &staticDataMetaHelper{}
	jdj.helper.setRecord(jdj.r)
	jdj.helper.setCb(jdj)
}

type staticDataJob struct {
	assistJob
	ns         map[string][]string
	nsCount    map[string]int64
	finishedNs []string
	equal      bool

	productCh chan *productItem
	nsList    *utils.TSList

	productWg sync.WaitGroup
	consumeWg sync.WaitGroup

	productRoutineManager *routineManager
	consumeRoutineManager *routineManager

	helper staticDataHelper

	ph *progressHelper
}

func (sd *staticDataJob) name() string {
	return reflect.TypeOf(staticDataJob{}).Name()
}

func (sd *staticDataJob) polymorphism() {
	sd.helper = &staticDataBaseHelper{}
	sd.helper.setRecord(sd.r)
	sd.helper.setCb(sd)
}

func (sd *staticDataJob) init() error {
	var ok bool
	sd.ns, ok = sd.getData("srcNsCleaned").(map[string][]string)
	if !ok || sd.ns == nil {
		sd.log.Error("get namespace data error")
		return fmt.Errorf("get namespace data error")
	}
	sd.nsCount, ok = sd.getData("srcCount").(map[string]int64)
	if !ok || sd.nsCount == nil {
		sd.log.Warn("can not get count data")
	}

	if sd.parameter.RunMode == "resume" {
		sd.finishedNs, ok = sd.getData("finishedNs").([]string)
		if !ok || sd.finishedNs == nil {
			sd.log.Error("get finished ns error")
			return fmt.Errorf("get finished ns error")
		}
	}

	sd.productCh = make(chan *productItem, sd.parameter.SrcConcurrency*buffLengthRatio)

	sd.productRoutineManager = &routineManager{}
	sd.consumeRoutineManager = &routineManager{}

	sd.nsList = utils.NewSTList()

	ts, ok := sd.getData("ts").(int64)
	if !ok || ts == 0 {
		ts = time.Now().Unix()
		sd.setData("ts", ts)
	}

	sd.newProgressHelper()
	sd.equal = true

	if err := sd.updateStep(sd.helper.name()); err != nil {
		sd.log.Errorf("update step error: %s", err.Error())
		return err
	}

	s := map[string]interface{}{"ts": ts}
	if len(sd.finishedNs) != 0 {
		s["finish_ns"] = sd.finishedNs
	}
	if err := sd.r.updateStatus(sd.task.id, s); err != nil {
		sd.log.Errorf("update status error: %s", err.Error())
		return err
	}

	return nil
}

func (sd *staticDataJob) newProgressHelper() {
	var docCount int64
	var nsCount int
	var doneDocCount int64
	for ns, cnt := range sd.nsCount {
		nsCount++
		docCount += cnt
		if inList(ns, sd.finishedNs) {
			doneDocCount += cnt
		}
	}

	sd.ph = &progressHelper{
		nsCount:     sd.nsCount,
		ns:          make(map[string]int64),
		totalCount:  docCount,
		totalNsNum:  int32(nsCount),
		finishCount: doneDocCount,
		finishNsNum: int32(len(sd.finishedNs)),
		nsFinishCb:  sd.recordFinishNs,
	}
}

func (sd *staticDataJob) compareItem(src, dst bson.M) bool {
	return sd.helper.compareItem(src, dst)
}

func (sd *staticDataJob) saveDiff(di *diffItem, src, dst bson.M) error {
	return sd.helper.saveDiff(di, src, dst)
}

func (sd *staticDataJob) diff(ctx context.Context, item *productItem) error {
	var equal = true
	res := sd.dstClient.Database(item.db).Collection(item.collection).FindOne(ctx, bson.D{{"_id", item.id}},
		options.FindOne().SetMaxTime(timeout))
	if res.Err() != nil && res.Err() != mongo.ErrNoDocuments {
		return res.Err()
	}
	var dstVal bson.M
	if res.Err() == mongo.ErrNoDocuments {
		// fmt.Printf("src dirty not found, ns: %s.%s, val: %v\n", item.db, item.collection, item.val)
		equal = false
	} else {
		if err := res.Decode(&dstVal); err != nil {
			return err
		}
		if !sd.compareItem(item.val, dstVal) {
			// fmt.Println("src dirty not equal: ", item.val, "\t", dstVal)
			equal = false
		}
	}
	if !equal {
		di := &diffItem{
			Ns: fmt.Sprintf("%s.%s", item.db, item.collection),
			Id: item.id,
		}
		if err := sd.saveDiff(di, item.val, dstVal); err != nil {
			sd.log.Errorf("save diff error: %s", err.Error())
			return err
		}
		sd.equal = false
	}
	// fmt.Println("diff: ", equal, "\t", item.db, "\t", item.collection, "\t", item.val)
	return nil
}

func (sd *staticDataJob) newProductRoutine(ctx context.Context, cancel context.CancelFunc) {
	sd.log.Info("create a new product routine")
	sd.productWg.Add(1)
	go func() {
		unit := newRoutineUnit()
		sd.productRoutineManager.add(unit)
		defer func() {
			sd.productRoutineManager.remove(unit)
			sd.productWg.Done()
			sd.log.Info("a product routine gone")
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				break
			}
			e := sd.nsList.Pop()
			if e == nil {
				return
			}
			item := e.(nsReader)
			sd.log.Info("fetch a new ns : ", item.getDb(), "\t", item.getCollection())
			unit.setPriorityFunc(item.count)
			ns := fmt.Sprintf("%s.%s", item.getDb(), item.getCollection())
			sd.ph.addNs(ns)
			if err := item.init(); err != nil {
				sd.setError(err)
				cancel()
				return
			}
			c, err := item.read(ctx, unit)
			if err != nil {
				sd.setError(err)
				cancel()
				return
			}
			if !c {
				sd.nsList.Push(sd.newNsReader(item.getDb(), item.getCollection()))
				sd.ph.deleteNs(ns)
				return
			}
		}
	}()
}

func (sd *staticDataJob) produce(ctx context.Context, cancel context.CancelFunc) {
	for i := 0; i < sd.parameter.SrcConcurrency; i++ {
		sd.newProductRoutine(ctx, cancel)
	}
	sd.productWg.Wait()
	return
}

func (sd *staticDataJob) newConsumeRoutine(ctx context.Context, cancel context.CancelFunc) {
	sd.log.Info("create a new consume routine")
	sd.consumeWg.Add(1)
	go func() {
		unit := newRoutineUnit()
		sd.consumeRoutineManager.add(unit)
		defer func() {
			sd.consumeRoutineManager.remove(unit)
			sd.consumeWg.Done()
			sd.log.Info("a consume routine gone")
		}()
		for {
			if unit.shouldExit() {
				sd.log.Info("consume routine will exit by routine manager")
				return
			}
			select {
			case <-ctx.Done():
				return
			case item, ok := <-sd.productCh:
				if !ok {
					return
				}
				if err := sd.diff(ctx, item); err != nil {
					sd.log.Error("consume routine will exit with error: ", err.Error())
					sd.setError(err)
					cancel()
					return
				}
				sd.ph.addCount(fmt.Sprintf("%s.%s", item.db, item.collection))
			}
		}
	}()
}

func (sd *staticDataJob) consume(ctx context.Context, cancel context.CancelFunc) {
	for i := 0; i < sd.parameter.DstConcurrency; i++ {
		sd.newConsumeRoutine(ctx, cancel)
	}
	sd.consumeWg.Wait()
}

func (sd *staticDataJob) readDone() {
	close(sd.productCh)
}

func (sd *staticDataJob) recordFinishNs(ns string) {
	if err := sd.r.recordFinishNs(sd.task.id, ns); err != nil {
		sd.log.Errorf("record finish ns error: %s, %s", ns, err.Error())
	}
}

func (sd *staticDataJob) adjustConcurrency(ctx context.Context, cancel context.CancelFunc) {
	{
		current := sd.productRoutineManager.len()
		desired := sd.parameter.SrcConcurrency
		if current > desired {
			sd.productRoutineManager.sort()
			sd.productRoutineManager.destroyN(current - desired)
		} else if desired > current {
			for i := 0; i < desired-current; i++ {
				sd.newProductRoutine(ctx, cancel)
			}
		}
	}

	{
		current := sd.consumeRoutineManager.len()
		desired := sd.parameter.DstConcurrency
		if current > desired {
			sd.consumeRoutineManager.destroyN(current - desired)
		} else if desired > current {
			for i := 0; i < desired-current; i++ {
				sd.newConsumeRoutine(ctx, cancel)
			}
		}
	}
}

func (sd *staticDataJob) startAdjustRoutine(ctx context.Context, cancel context.CancelFunc) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second * 5):
				if sd.parameter.dirty {
					sd.adjustConcurrency(ctx, cancel)
					sd.parameter.dirty = false
				}
			}
		}
	}()
}

func (sd *staticDataJob) startFlushProgressRoutine(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second * 5):
				sd.flushProgress(ctx)
			}
		}
	}()
}

func (sd *staticDataJob) flushProgress(ctx context.Context) {
	if err := sd.r.flushProgress(ctx, sd.task.id, sd.ph.getProgressPercent(), sd.ph.getNsProgress()); err != nil {
		sd.log.Errorf("flush progress error: %s", err.Error())
	}
}

func (sd *staticDataJob) do() (bool, error) {
	sd.log.Info("start static data compare")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer func() {
			wg.Done()
			sd.readDone()
		}()
		sd.produce(ctx, cancel)
	}()
	go func() {
		defer wg.Done()
		sd.consume(ctx, cancel)
	}()

	sd.startAdjustRoutine(ctx, cancel)
	sd.startFlushProgressRoutine(ctx)

	for db, coll := range sd.ns {
		for _, c := range coll {
			ns := fmt.Sprintf("%s.%s", db, c)
			if inList(ns, sd.finishedNs) {
				continue
			}
			sd.nsList.Push(sd.newNsReader(db, c))
		}
	}
	wg.Wait()

	if sd.error() != nil {
		return false, sd.error()
	}

	result := &compareResult{
		Task:      sd.tName,
		Step:      sd.helper.name(),
		Identical: "yes",
	}
	if !sd.equal {
		result.Identical = "no"
	}
	if err := sd.r.saveResult(result); err != nil {
		return false, err
	}
	sd.flushProgress(context.Background())
	sd.log.Info("static data compare finish")
	return true, nil
}

func (sd *staticDataJob) newNsReader(db, collection string) nsReader {
	ns := fmt.Sprintf("%s.%s", db, collection)
	count := sd.nsCount[ns]
	if sd.parameter.Sample == 0 {
		return &nsBaseReader{
			c:          sd.srcClient,
			db:         db,
			collection: collection,
			log:        sd.log,
			r:          sd.r,
			f:          sd,
			totalCount: count,
		}
	}
	sampleSize := count * int64(sd.parameter.Sample) / 100
	if sampleSize < 1 {
		sampleSize = 1
	}
	return &nsSampleReader{
		nsBaseReader: &nsBaseReader{
			c:          sd.srcClient,
			db:         db,
			collection: collection,
			log:        sd.log,
			r:          sd.r,
			f:          sd,
			totalCount: count,
		},
		sampleSize: sampleSize,
	}
}

type productItem struct {
	db         string
	collection string
	id         interface{}
	val        bson.M
}

type nsReader interface {
	init() error
	read(ctx context.Context, unit *routineUint) (bool, error)
	getDb() string
	getCollection() string
	count() int
}

type nsBaseReader struct {
	c          *mongo.Client
	db         string
	collection string
	log        *logrus.Logger
	r          *record
	f          *staticDataJob
	totalCount int64

	cnt    int64
	cursor *mongo.Cursor
}

func (nr *nsBaseReader) getDb() string {
	return nr.db
}

func (nr *nsBaseReader) getCollection() string {
	return nr.collection
}

func (nr *nsBaseReader) init() error {
	var err error
	nr.cursor, err = nr.c.Database(nr.db).Collection(nr.collection).Find(context.Background(), bson.D{})
	return err
}

func (nr *nsBaseReader) read(ctx context.Context, unit *routineUint) (bool, error) {
	fmt.Println("start read: ", nr.db, "\t", nr.collection)
	nr.log.Infof("start read: %s, %s", nr.db, nr.collection)
	for nr.cursor.Next(ctx) {
		if unit.shouldExit() {
			nr.log.Warnf("product goroutine exit")
			return false, nil
		}
		var val bson.M
		if err := nr.cursor.Decode(&val); err != nil {
			return false, err
		}
		id, ok := val["_id"]
		if !ok {
			nr.log.Errorf("id not found in source, ns: %s.%s, doc: %v", nr.db, nr.collection, val)
			return false, fmt.Errorf("id not found")
		}
		item := &productItem{
			db:         nr.db,
			collection: nr.collection,
			id:         id,
			val:        val,
		}
		nr.send(item)
	}
	nr.log.Infof("read finish: %s, %s", nr.db, nr.collection)
	return true, nil
}
func (nr *nsBaseReader) send(item *productItem) {
	nr.f.productCh <- item
}

func (nr *nsBaseReader) count() int {
	if nr.cnt > math.MaxInt {
		return math.MaxInt
	}
	return int(nr.cnt)
}

type nsSampleReader struct {
	*nsBaseReader
	sampleSize int64
}

func (nsr *nsSampleReader) init() error {
	var err error
	pipeline := []bson.D{{{"$sample", bson.D{{"size", nsr.sampleSize}}}}}
	nsr.cursor, err = nsr.c.Database(nsr.db).Collection(nsr.collection).Aggregate(context.Background(), pipeline)
	return err
}
