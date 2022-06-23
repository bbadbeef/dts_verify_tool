package compare

import (
	"context"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"mongo_compare/utils"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
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
	clientOptions := options.Client().ApplyURI(uri).SetMaxPoolSize(maxConnPollSize).SetMaxConnIdleTime(time.Minute)
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

func parseOplog(doc *oplogDoc) *diffItem {
	di := &diffItem{
		Ns: doc.Ns,
		Ts: doc.Ts.Unix(),
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

func diffNs(src, dst map[string][]string) (bool, map[string]interface{}) {
	srcNs := make(map[string]struct{})
	dstNs := make(map[string]struct{})
	for db, coll := range src {
		for _, c := range coll {
			srcNs[fmt.Sprintf("%s.%s", db, c)] = struct{}{}
		}
	}
	for db, coll := range dst {
		for _, c := range coll {
			ns := fmt.Sprintf("%s.%s", db, c)
			if _, ok := srcNs[ns]; ok {
				delete(srcNs, ns)
				continue
			}
			dstNs[ns] = struct{}{}
		}
	}
	resSrc := make([]string, 0)
	resDst := make([]string, 0)
	for k, _ := range srcNs {
		resSrc = append(resSrc, k)
	}
	for k, _ := range dstNs {
		resDst = append(resDst, k)
	}
	return len(resSrc) == 0 && len(resDst) == 0, map[string]interface{}{
		"source":      resSrc,
		"destination": resDst,
	}
}

func diffCount(src, dst map[string]int64) map[string]interface{} {
	resSrc := make(map[string]int64)
	resDst := make(map[string]int64)
	for ns, count := range src {
		if dst[ns] == count {
			continue
		}
		resSrc[ns] = count
		resDst[ns] = dst[ns]
	}
	for ns, count := range dst {
		if src[ns] == count {
			continue
		}
		resSrc[ns] = src[ns]
		resDst[ns] = dst[ns]
	}
	return map[string]interface{}{
		"source":      resSrc,
		"destination": resDst,
	}
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
