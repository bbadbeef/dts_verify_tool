package compare

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"sync"
)

type staticDataHelper interface {
	compareItem(src, dst bson.M) bool
	saveDiff(di *diffItem, src, dst bson.M) error
	setRecord(r *record)
	setCb(cb callbackType)
	name() string
}

type callbackType interface {
	name() string
}

type staticDataBaseHelper struct {
	r  *record
	cb callbackType
}

func (sdb *staticDataBaseHelper) setRecord(r *record) {
	sdb.r = r
}

func (sdb *staticDataBaseHelper) setCb(cb callbackType) {
	sdb.cb = cb
}

func (sdb *staticDataBaseHelper) name() string {
	return sdb.cb.name()
}

func (sdb *staticDataBaseHelper) compareItem(src, dst bson.M) bool {
	return compareDoc(src, dst)
}

func (sdb *staticDataBaseHelper) saveDiff(di *diffItem, src, dst bson.M) error {
	return sdb.r.saveDiff([]interface{}{di})
}

type staticDataMetaHelper struct {
	staticDataBaseHelper
}

func (sdb *staticDataMetaHelper) saveDiff(di *diffItem, src, dst bson.M) error {
	return sdb.r.saveMetaDiff([]interface{}{di})
}

type staticDataAccountHelper struct {
	staticDataMetaHelper
}

func (sdb *staticDataAccountHelper) compareItem(src, dst bson.M) bool {
	return compareAccount(src, dst)
}

type staticDataShardHelper struct {
	staticDataMetaHelper
}

func (sds *staticDataShardHelper) compareItem(src, dst bson.M) bool {
	return compareShard(src, dst)
}

type progressHelper struct {
	totalCount  int64
	finishCount int64
	totalNsNum  int32
	finishNsNum int32
	nsCount     map[string]int64
	ns          map[string]int64
	nsFinishCb  func(ns string)
	sync.Mutex
}

func (ph *progressHelper) addCount(ns string) {
	ph.Lock()
	defer ph.Unlock()
	if _, ok := ph.ns[ns]; !ok {
		return
	}
	ph.finishCount++
	ph.ns[ns]++
	if ph.ns[ns] == ph.nsCount[ns] {
		ph.finishNsNum++
		ph.nsFinishCb(ns)
	}
}

func (ph *progressHelper) addNs(ns string) {
	ph.Lock()
	ph.ns[ns] = 0
	ph.Unlock()
}

func (ph *progressHelper) deleteNs(ns string) {
	ph.Lock()
	delete(ph.ns, ns)
	ph.Unlock()
}

func (ph *progressHelper) getProgressPercent() int {
	if ph.totalCount == 0 {
		return 0
	}
	p := int(ph.finishCount * 100 / ph.totalCount)
	if p > 100 {
		p = 100
	}
	return p
}

func (ph *progressHelper) getNsProgress() string {
	return fmt.Sprintf("%d/%d", ph.finishNsNum, ph.totalNsNum)
}
