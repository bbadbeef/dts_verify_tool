package compare

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/bbadbeef/dts_verify_tool/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	maxConnPollSize = 5000
	timeout         = 20 * time.Second
	retryTimes      = 5
	retryDuration   = 10 * time.Second
	buffLengthRatio = 100
)

const (
	StatusRunning    = "running"
	StatusFailed     = "failed"
	StatusTerminated = "terminated"
	StatusSuccess    = "success"
)

var consoleStatus = map[string]string{
	StatusRunning:    utils.Green(StatusRunning),
	StatusFailed:     utils.Red(StatusFailed),
	StatusTerminated: utils.Yellow(StatusTerminated),
	StatusSuccess:    utils.Green(StatusSuccess),
}

var (
	systemDb         = []string{"admin", "local", "config"}
	systemCollection = []string{"system.*"}
)

// MetaDiffItem ...
type MetaDiffItem struct {
	Ns     string
	SrcId  interface{}
	SrcVal interface{}
	DstId  interface{}
	DstVal interface{}
}

type oplogDoc struct {
	Op string
	Ns string
	O2 bson.M
	O  bson.M
	Ts time.Time
}

type accountDoc struct {
	Id    string `bson:"_id"`
	Roles []struct {
		Role string
		Db   string
	} `bson:"roles"`
}

func (doc *accountDoc) sort() {
	sort.Slice(doc.Roles, func(i, j int) bool {
		return doc.Roles[i].Role > doc.Roles[j].Role
	})
}

type shardDoc struct {
	Id      string `json:"_id"`
	Dropped bool
	key     map[string]interface{}
	Unique  bool
}

func newMongoClient(uri string) (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI(uri).SetMaxPoolSize(maxConnPollSize).SetMaxConnIdleTime(time.Minute).
		SetReadPreference(readpref.SecondaryPreferred())
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func showDbs(c *mongo.Client) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.ListDatabaseNames(ctx, bson.D{})
}

func showCollections(c *mongo.Client, db string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.Database(db).ListCollectionNames(ctx, bson.D{})
}

func isView(c *mongo.Client, db, coll string) bool {
	err := c.Database(db).RunCommand(context.Background(), bson.M{"collStats": coll}).Err()
	if err != nil {
		return true
	}
	return false
}

func getIndexes(ctx context.Context, c *mongo.Client, db, coll string) (map[string]map[string]interface{}, error) {
	subCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cursor, err := c.Database(db).Collection(coll).Indexes().List(subCtx, options.ListIndexes().SetMaxTime(timeout))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	var results []bson.M
	if err = cursor.All(subCtx, &results); err != nil {
		return nil, err
	}
	idxMap := make(map[string]map[string]interface{})
	for _, idx := range results {
		delete(idx, "ns")
		delete(idx, "background")
		if name, ok := idx["name"].(string); ok {
			idxMap[name] = idx
		}
	}
	return idxMap, nil
}

func findById(ctx context.Context, c *mongo.Client, ns string, id interface{}) (bson.M, error) {
	subCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	nss := strings.SplitN(ns, ".", 2)
	if len(nss) != 2 {
		return nil, fmt.Errorf("illegal namespace: %s", ns)
	}
	res := c.Database(nss[0]).Collection(nss[1]).FindOne(subCtx, bson.D{{"_id", id}},
		options.FindOne().SetMaxTime(timeout))
	if res.Err() != nil {
		if res.Err() != mongo.ErrNoDocuments {
			return nil, res.Err()
		}
		return nil, nil
	}
	var val bson.M
	if err := res.Decode(&val); err != nil {
		return nil, err
	}
	return val, nil
}

func findByIdWithRetry(ctx context.Context, c *mongo.Client, ns string,
	id interface{}) (bson.M, error) {
	var res bson.M
	var err error
	for i := 0; i < retryTimes; i++ {
		res, err = findById(ctx, c, ns, id)
		if err != nil {
			time.Sleep(retryDuration)
			continue
		}
		break
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

func filterOplog(oplog *oplogDoc, dbs, nss []string) bool {
	//if oplog.Op != "i" && oplog.Op != "u" && oplog.Op != "d" {
	//	return false
	//}
	if len(oplog.Ns) == 0 {
		return false
	}
	ns := strings.SplitN(oplog.Ns, ".", 2)
	if len(ns) != 2 {
		return false
	}
	if inList(ns[0], systemDb) {
		return false
	}
	if inListRegex(ns[1], systemCollection) {
		return false
	}
	if len(dbs) != 0 {
		if !inList(ns[0], dbs) {
			return false
		}
	}
	if len(nss) != 0 {
		if !inList(oplog.Ns, nss) {
			return false
		}
	}
	return true
}

func parseOplog(doc *oplogDoc) *DiffItem {
	di := &DiffItem{
		Ns:        doc.Ns,
		Ts:        doc.Ts.Unix(),
		Confirmed: false,
	}
	if id, ok := doc.O2["_id"]; ok {
		di.Id = id
	} else if id, ok := doc.O["_id"]; ok {
		di.Id = id
	} else {
		return nil
	}
	return di
}

func compareDoc(a, b bson.M) bool {
	return reflect.DeepEqual(a, b)
}

func compareAccount(a, b bson.M) bool {
	ab, _ := bson.Marshal(a)
	bb, _ := bson.Marshal(b)
	var src, dst accountDoc
	if err := bson.Unmarshal(ab, &src); err != nil {
		return false
	}
	if err := bson.Unmarshal(bb, &dst); err != nil {
		return false
	}
	src.sort()
	dst.sort()
	if strings.HasPrefix(src.Id, "cmgo-") || strings.HasPrefix(dst.Id, "cmgo-") {
		return true
	}
	return reflect.DeepEqual(src, dst)
}

func compareShard(a, b bson.M) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	var src, dst shardDoc
	if err := json.Unmarshal(ab, &src); err != nil {
		return false
	}
	if err := json.Unmarshal(bb, &dst); err != nil {
		return false
	}
	return reflect.DeepEqual(src, dst)
}

func diffNs(src, dst map[string][]string) []*MetaDiffItem {
	srcNs := make(map[string]struct{})
	dstNs := make(map[string]struct{})
	for db, coll := range src {
		for _, c := range coll {
			srcNs[fmt.Sprintf("%s.%s", db, c)] = struct{}{}
		}
	}
	for db, coll := range dst {
		for _, c := range coll {
			dstNs[fmt.Sprintf("%s.%s", db, c)] = struct{}{}
		}
	}
	res := make([]*MetaDiffItem, 0)
	for k, _ := range srcNs {
		if _, ok := dstNs[k]; !ok {
			res = append(res, &MetaDiffItem{
				Ns:    k,
				SrcId: k,
				DstId: "",
			})
		}
	}
	for k, _ := range dstNs {
		if _, ok := srcNs[k]; !ok {
			res = append(res, &MetaDiffItem{
				Ns:    k,
				SrcId: "",
				DstId: k,
			})
		}
	}
	return res
}

func diffCount1(src, dst map[string]int64) ([]*MetaDiffItem, []*MetaDiffItem, []*MetaDiffItem) {
	schemaRes := make([]*MetaDiffItem, 0)
	countRes := make([]*MetaDiffItem, 0)
	dataRes := make([]*MetaDiffItem, 0)
	for ns, count := range src {
		dstCount, ok := dst[ns]
		if !ok {
			schemaRes = append(schemaRes, &MetaDiffItem{
				Ns:    ns,
				SrcId: ns,
				DstId: "",
			})
			if true || count != 0 {
				countRes = append(countRes, &MetaDiffItem{
					Ns:     ns,
					SrcId:  "",
					DstId:  "",
					SrcVal: count,
					DstVal: "null",
				})
			}
			continue
		}
		if dstCount != count {
			countRes = append(countRes, &MetaDiffItem{
				Ns:     ns,
				SrcId:  "",
				DstId:  "",
				SrcVal: count,
				DstVal: dstCount,
			})
		}
	}
	for ns, count := range dst {
		if _, ok := src[ns]; ok {
			continue
		}
		schemaRes = append(schemaRes, &MetaDiffItem{
			Ns:    ns,
			SrcId: "",
			DstId: ns,
		})
		if true || count != 0 {
			countRes = append(countRes, &MetaDiffItem{
				Ns:     ns,
				SrcId:  "",
				DstId:  "",
				SrcVal: "null",
				DstVal: count,
			})
			dataRes = append(dataRes, &MetaDiffItem{
				Ns:    ns,
				SrcId: "null",
				DstId: "所有数据",
			})
		}
	}
	return schemaRes, countRes, dataRes
}

func diffCount(src, dst map[string]int64) []*MetaDiffItem {
	res := make([]*MetaDiffItem, 0)
	for ns, count := range src {
		if dst[ns] == count {
			continue
		}
		res = append(res, &MetaDiffItem{
			Ns:     ns,
			SrcId:  "",
			DstId:  "",
			SrcVal: count,
			DstVal: dst[ns],
		})
	}
	for ns, count := range dst {
		if _, ok := src[ns]; ok {
			continue
		}
		res = append(res, &MetaDiffItem{
			Ns:     ns,
			SrcId:  "",
			DstId:  "",
			SrcVal: 0,
			DstVal: count,
		})
	}
	return res
}

func diffIndex(src, dst map[string]map[string]map[string]interface{}) []*MetaDiffItem {
	res := make([]*MetaDiffItem, 0)
	for ns, srcIndexMap := range src {
		for srcIndexName, srcIndexItem := range srcIndexMap {
			if dst[ns] == nil || dst[ns][srcIndexName] == nil {
				res = append(res, &MetaDiffItem{
					Ns:     ns,
					SrcId:  srcIndexName,
					SrcVal: srcIndexItem,
					DstId:  "",
					DstVal: "",
				})
				continue
			}
			if compareDoc(srcIndexItem, dst[ns][srcIndexName]) {
				continue
			}
			res = append(res, &MetaDiffItem{
				Ns:     ns,
				SrcId:  srcIndexName,
				SrcVal: srcIndexItem,
				DstId:  srcIndexName,
				DstVal: dst[ns][srcIndexName],
			})
		}
	}
	for ns, dstIndexMap := range dst {
		for dstIndexName, dstIndexItem := range dstIndexMap {
			if src[ns] == nil || src[ns][dstIndexName] == nil {
				res = append(res, &MetaDiffItem{
					Ns:     ns,
					SrcId:  "",
					SrcVal: "",
					DstId:  dstIndexName,
					DstVal: dstIndexItem,
				})
				continue
			}
		}
	}
	return res
}

func inList(item string, list []string) bool {
	for _, it := range list {
		if item == it {
			return true
		}
	}
	return false
}

func inListRegex(item string, list []string) bool {
	for _, it := range list {
		b, err := regexp.MatchString(it, item)
		if err != nil {
			return false
		}
		if b {
			return true
		}
	}
	return false
}

func deepCopy(m map[string][]string) (res map[string][]string) {
	b, _ := json.Marshal(&m)
	json.Unmarshal(b, &res)
	return
}

func truncatedStr(raw []byte) string {
	if len(raw) > 1024 {
		raw = raw[:1024]
		raw = append(raw, []byte("...")...)
	}
	return string(raw)
}

func marshalId(id interface{}) string {
	if id == nil {
		return ""
	}
	switch id.(type) {
	case primitive.ObjectID:
		return id.(primitive.ObjectID).String()
	default:
		return fmt.Sprintf("%v", id)
	}
}

func marshalVal(val interface{}) string {
	if val == nil {
		return ""
	}
	switch val.(type) {
	case string, int, uint, int64, uint64:
		return fmt.Sprintf("%v", val)
	default:
	}
	v, _ := bson.MarshalExtJSON(val, true, true)
	return truncatedStr(v)
}
