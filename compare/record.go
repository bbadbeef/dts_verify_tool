package compare

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"strings"
	"time"
)

const (
	diffCollection     = "diff"
	diffMetaCollection = "diff_meta"
	statusCollection   = "status"
	resultCollection   = "result"
)

// Record for export
type Record struct {
	*record
}

// NewRecord ...
func NewRecord(srcUri, dstUri string, outDb string, taskId string) (*Record, error) {
	dstClient, err := newMongoClient(dstUri)
	if err != nil {
		return nil, err
	}
	srcClient, err := newMongoClient(srcUri)
	if err != nil {
		return nil, err
	}
	return &Record{
		&record{
			dstClient: dstClient,
			srcClient: srcClient,
			outDb:     outDb,
			taskId:    taskId,
		},
	}, nil
}

// JobResult ...
type JobResult struct {
	Task      string      `bson:"task,omitempty"`
	Step      string      `bson:"step,omitempty"`
	Identical bool        `bson:"identical,omitempty"`
	Diff      interface{} `bson:"diff,omitempty"`
}

// DiffItem ...
type DiffItem struct {
	OId       interface{} `bson:"_id,omitempty"`
	Ns        string
	Id        interface{} `bson:"id,omitempty"`
	Ts        int64
	Confirmed bool
	shard     int
}

// TaskStatus ...
type TaskStatus struct {
	OId         string `bson:"_id"`
	Status      string
	Name        string
	StartTime   string `bson:"start_time"`
	EndTime     string `bson:"end_time"`
	Extra       uint
	SubTask     string `bson:"sub_task,omitempty"`
	Step        string
	Progress    int      `bson:"progress,omitempty"`
	FinishNs    []string `bson:"finish_ns,omitempty"`
	FinishNsCnt string   `bson:"finish_ns_cnt,omitempty"`
	Ts          int64    `bson:"ts,omitempty"`
	Error       string   `bson:"error,omitempty"`
	Identical   bool     `bson:"-"`
}

type record struct {
	dstClient *mongo.Client
	srcClient *mongo.Client
	outDb     string
	taskId    string
}

func (r *record) diffCollection() string {
	return fmt.Sprintf("%s_%s", diffCollection, r.taskId)
}

func (r *record) diffMetaCollection() string {
	return fmt.Sprintf("%s_%s", diffMetaCollection, r.taskId)
}

func (r *record) statusCollection() string {
	return fmt.Sprintf("%s_%s", statusCollection, r.taskId)
}

func (r *record) resultCollection() string {
	return fmt.Sprintf("%s_%s", resultCollection, r.taskId)
}

func (r *record) init() error {
	if err := r.clean(); err != nil {
		return err
	}
	keysDoc := bsonx.Doc{}
	keysDoc = keysDoc.Append("ns", bsonx.Int32(1)).Append("id", bsonx.Int32(1))
	im := mongo.IndexModel{
		Keys:    keysDoc,
		Options: options.Index().SetBackground(true).SetUnique(true),
	}
	if err := r.dstClient.Database(r.outDb).CreateCollection(context.Background(), r.diffCollection()); err != nil {
		return err
	}
	_, err := r.dstClient.Database(r.outDb).Collection(r.diffCollection()).Indexes().
		CreateOne(context.Background(), im)
	if err != nil {
		return err
	}

	keysDoc = bsonx.Doc{}
	keysDoc = keysDoc.Append("ts", bsonx.Int32(1))
	im = mongo.IndexModel{
		Keys:    keysDoc,
		Options: options.Index().SetBackground(true),
	}
	_, err = r.dstClient.Database(r.outDb).Collection(r.diffCollection()).Indexes().
		CreateOne(context.Background(), im)

	keysDoc = bsonx.Doc{}
	keysDoc = keysDoc.Append("confirmed", bsonx.Int32(1))
	im = mongo.IndexModel{
		Keys:    keysDoc,
		Options: options.Index().SetBackground(true),
	}
	_, err = r.dstClient.Database(r.outDb).Collection(r.diffCollection()).Indexes().
		CreateOne(context.Background(), im)
	return err
}

func (r *record) saveResult(result *JobResult) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := r.dstClient.Database(r.outDb).Collection(r.resultCollection()).InsertOne(ctx, result)
	return err
}

func (r *record) clean() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	r.dstClient.Database(r.outDb).Collection(diffCollection).Drop(ctx)
	r.dstClient.Database(r.outDb).Collection(diffMetaCollection).Drop(ctx)
	r.dstClient.Database(r.outDb).Collection(statusCollection).Drop(ctx)
	r.dstClient.Database(r.outDb).Collection(resultCollection).Drop(ctx)
	return nil
}

func (r *record) saveMetaDiff(items []interface{}) error {
	return r.saveDiffBase(items, r.diffMetaCollection())
}

func (r *record) saveDiff(items []interface{}) error {
	return r.saveDiffBase(items, r.diffCollection())
}

func (r *record) saveDiffBase(items []interface{}, coll string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err := r.dstClient.Database(r.outDb).Collection(coll).
		InsertMany(ctx, items, options.InsertMany().SetOrdered(false))
	if bwErr, ok := err.(mongo.BulkWriteException); ok {
		if bwErr.WriteConcernError != nil {
			return err
		}
		for _, wErr := range bwErr.WriteErrors {
			if wErr.Code != 11000 {
				return err
			}
		}
	} else {
		return err
	}
	return nil
}

func (r *record) removeDiff(id interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err := r.dstClient.Database(r.outDb).Collection(r.diffCollection()).DeleteOne(ctx, bson.D{{"_id", id}})
	return err
}

func (r *record) confirmDiff(id interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	filter := bson.D{{"_id", id}}
	update := bson.D{{"$set", bson.D{{"confirmed", true}}}}
	_, err := r.dstClient.Database(r.outDb).Collection(r.diffCollection()).UpdateOne(ctx, filter, update)
	return err
}

func (r *record) saveStatus(status *TaskStatus) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := r.dstClient.Database(r.outDb).Collection(r.statusCollection()).FindOne(ctx, bson.D{{"_id", status.OId}}).
		Err()
	if err == mongo.ErrNoDocuments {
		if _, err := r.dstClient.Database(r.outDb).Collection(r.statusCollection()).InsertOne(ctx, status); err != nil {
			return err
		}
		return nil
	}

	if err == nil {
		if _, err := r.dstClient.Database(r.outDb).Collection(r.statusCollection()).ReplaceOne(ctx,
			bson.D{{"_id", status.OId}}, status); err != nil {
			return err
		}
		return nil
	}
	return err
}

func (r *record) recordFinishNs(taskId string, ns string) error {
	update := bson.M{"$push": bson.M{"finish_ns": ns}}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err := r.dstClient.Database(r.outDb).Collection(r.statusCollection()).
		UpdateOne(ctx, bson.D{{"_id", taskId}}, update)
	return err
}

func (r *record) updateStatus(taskId string, fields map[string]interface{}) error {
	update := bson.M{"$set": fields}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err := r.dstClient.Database(r.outDb).Collection(r.statusCollection()).
		UpdateOne(ctx, bson.D{{"_id", taskId}}, update)
	return err
}

// GetStatus ...
func (r *record) GetStatus() (*TaskStatus, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	res := r.dstClient.Database(r.outDb).Collection(r.statusCollection()).FindOne(ctx, bson.D{},
		options.FindOne().SetMaxTime(timeout))
	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			fmt.Println("get no status")
			return nil, nil
		}
		return nil, res.Err()
	}
	var status TaskStatus
	if err := res.Decode(&status); err != nil {
		return nil, err
	}

	idential, err := r.IsIdentical()
	if err != nil {
		return nil, err
	}
	status.Identical = idential
	return &status, nil
}

// GetDiffCount ...
func (r *record) GetDiffCount() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return r.dstClient.Database(r.outDb).Collection(r.diffCollection()).EstimatedDocumentCount(ctx,
		options.EstimatedDocumentCount().SetMaxTime(timeout))
}

// IsIdentical ...
func (r *record) IsIdentical() (bool, error) {
	results, err := r.GetResult()
	if err != nil {
		return false, err
	}
	for _, result := range results {
		if result.Identical == false {
			return false, nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		singleRes := r.dstClient.Database(r.outDb).Collection(r.diffCollection()).FindOne(ctx, bson.D{{"confirmed", true}},
			options.FindOne().SetMaxTime(timeout))
		if singleRes.Err() != nil {
			if singleRes.Err() == mongo.ErrNoDocuments {
				break
			}
			return false, singleRes.Err()
		}
		return false, nil
	}

	for {
		singleRes := r.dstClient.Database(r.outDb).Collection(r.diffMetaCollection()).FindOne(ctx, bson.D{{}},
			options.FindOne().SetMaxTime(timeout))
		if singleRes.Err() != nil {
			if singleRes.Err() == mongo.ErrNoDocuments {
				return true, nil
			}
			return false, singleRes.Err()
		}
		return false, nil
	}
}

// GetSampleDiffData ...
func (r *record) GetSampleDiffData() ([]*MetaDiffItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout*3)
	defer cancel()

	cursor, err := r.dstClient.Database(r.outDb).Collection(r.diffCollection()).
		Find(ctx, bson.D{{"confirmed", true}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	sampleSize := 1000
	res := make([]*MetaDiffItem, 0)
	for cursor.Next(ctx) {
		if sampleSize <= 0 {
			break
		}
		sampleSize--
		var di DiffItem
		if err := cursor.Decode(&di); err != nil {
			return nil, err
		}
		srcData, err := findById(ctx, r.srcClient, di.Ns, di.Id)
		if err != nil {
			return nil, err
		}
		dstData, err := findById(ctx, r.dstClient, di.Ns, di.Id)
		if err != nil {
			return nil, err
		}
		item := &MetaDiffItem{
			Ns:    di.Ns,
			SrcId: marshalId(di.Id),
			DstId: marshalId(func() interface{} {
				if len(dstData) == 0 {
					return nil
				}
				return di.Id
			}()),
			SrcVal: marshalVal(srcData),
			DstVal: marshalVal(dstData),
		}
		res = append(res, item)
	}
	return res, nil
}

// GetResult ...
func (r *record) GetResult() ([]*JobResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cursor, err := r.dstClient.Database(r.outDb).Collection(r.resultCollection()).Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	res := make([]*JobResult, 0)
	defer cursor.Close(context.Background())
	for cursor.Next(ctx) {
		var cr JobResult
		if err := cursor.Decode(&cr); err != nil {
			return nil, err
		}
		res = append(res, &cr)
	}
	return res, nil
}

func (r *record) flushProgress(ctx context.Context, taskId string, progress int, nsProgress string) error {
	return r.updateStatus(taskId, map[string]interface{}{
		"progress":      progress,
		"finish_ns_cnt": nsProgress,
	})
}

// GetRecentTs ...
func (r *record) GetRecentTs() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	res := r.dstClient.Database(r.outDb).Collection(r.diffCollection()).FindOne(ctx, bson.D{}, options.FindOne().
		SetSort(bson.D{{"ts", 1}}).SetMaxTime(timeout))
	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return -1, nil
		}
		return 0, res.Err()
	}
	var diff DiffItem
	if err := res.Decode(&diff); err != nil {
		return 0, err
	}
	return int(diff.Ts), nil
}

// GetAccountMetaDiff ...
func (r *record) GetAccountMetaDiff() ([]*DiffItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cursor, err := r.dstClient.Database(r.outDb).Collection(r.diffMetaCollection()).Find(ctx, bson.D{},
		options.Find().SetMaxTime(timeout))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	res := make([]*DiffItem, 0)
	for cursor.Next(ctx) {
		var di DiffItem
		if err := cursor.Decode(&di); err != nil {
			return nil, err
		}
		if di.Ns == "admin.system.users" {
			res = append(res, &di)
		}
	}
	return res, nil
}

// GetJsMetaDiff ...
func (r *record) GetJsMetaDiff() ([]*DiffItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cursor, err := r.dstClient.Database(r.outDb).Collection(r.diffMetaCollection()).Find(ctx, bson.D{},
		options.Find().SetMaxTime(timeout))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	res := make([]*DiffItem, 0)
	for cursor.Next(ctx) {
		var di DiffItem
		if err := cursor.Decode(&di); err != nil {
			return nil, err
		}
		if strings.Contains(di.Ns, ".system.js") {
			res = append(res, &di)
		}
	}
	return res, nil
}

// GetShardMetaDiff ...
func (r *record) GetShardMetaDiff() ([]*DiffItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cursor, err := r.dstClient.Database(r.outDb).Collection(r.diffMetaCollection()).Find(ctx, bson.D{},
		options.Find().SetMaxTime(timeout))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	res := make([]*DiffItem, 0)
	for cursor.Next(ctx) {
		var di DiffItem
		if err := cursor.Decode(&di); err != nil {
			return nil, err
		}
		if di.Ns == "config.collections" {
			res = append(res, &di)
		}
	}
	return res, nil
}

// TagDoc ...
type TagDoc struct {
	Ns  string
	Tag string
}

// GetTagsDiff ...
func (r *record) GetTagsDiff() ([]*TagDoc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cursor, err := r.dstClient.Database(r.outDb).Collection(r.diffMetaCollection()).Find(ctx, bson.D{},
		options.Find().SetMaxTime(timeout))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	res := make([]*TagDoc, 0)
	for cursor.Next(ctx) {
		var di DiffItem
		if err := cursor.Decode(&di); err != nil {
			return nil, err
		}
		srcData, err := findById(ctx, r.srcClient, di.Ns, di.Id)
		if err != nil {
			return nil, err
		}
		if di.Ns != "config.collections" {
			continue
		}
		var doc TagDoc
		doc.Ns, _ = srcData["ns"].(string)
		doc.Tag, _ = srcData["tag"].(string)
		res = append(res, &doc)
	}
	return res, nil
}

func (r *record) cleanForResume() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	tables := []string{r.resultCollection(), r.diffMetaCollection()}
	for _, table := range tables {
		if err := r.dstClient.Database(r.outDb).Collection(table).Drop(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *record) setTaskError(taskId string, err error) error {
	return r.updateStatus(taskId, map[string]interface{}{"status": StatusFailed, "error": err.Error()})
}

func (r *record) getOplogDelay() (int, error) {
	src, err := r.getSrcOplogTs()
	if err != nil {
		return 0, err
	}
	dst, err := r.getDstOplogTs()
	if err != nil {
		return 0, err
	}
	return int(src - dst), nil
}

func (r *record) getSrcOplogTs() (int64, error) {
	return r.getOplogTs(r.srcClient)
}

func (r *record) getDstOplogTs() (int64, error) {
	return r.getOplogTs(r.dstClient)
}

func (r *record) getOplogTs(c *mongo.Client) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cursor, err := c.Database("local").Collection("oplog.rs").Find(ctx,
		bson.D{{"op", bson.M{"$in": []string{"i", "u", "d"}}}},
		options.Find().SetSort(bson.D{{"ts", -1}}))
	if err != nil {
		return 0, err
	}
	for cursor.Next(ctx) {
		var od oplogDoc
		if err := cursor.Decode(&od); err != nil {
			return 0, err
		}
		if !filterOplog(&od, nil, nil) {
			continue
		}
		fmt.Println(od.Ts.Format("2006-01-02 15:04:05"), time.Now().Format("2006-01-02 15:04:05"))
		fmt.Println(od.Op, "\t", od.Ns, "\t", od.O, "\t", od.O2)
		return od.Ts.Unix(), nil
	}
	return 0, nil
}
