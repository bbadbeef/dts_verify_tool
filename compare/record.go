package compare

import (
	"context"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"strings"
)

const (
	diffCollection     = "diff"
	diffMetaCollection = "diff_meta"
	statusCollection   = "status"
	resultCollection   = "result"
)

type compareResult struct {
	Task      string      `bson:"task,omitempty"`
	Step      string      `bson:"step,omitempty"`
	Identical string      `bson:"identical,omitempty"`
	Diff      interface{} `bson:"diff,omitempty"`
}

type diffItem struct {
	OId   interface{} `bson:"_id,omitempty"`
	Ns    string
	Id    interface{} `bson:"id,omitempty"`
	Ts    int64
	shard int
}

type taskStatus struct {
	OId         string `bson:"_id"`
	Status      string
	Name        string
	SubTask     string `bson:"sub_task,omitempty"`
	Step        string
	Progress    int      `bson:"progress,omitempty"`
	FinishNs    []string `bson:"finish_ns,omitempty"`
	FinishNsCnt string   `bson:"finish_ns_cnt,omitempty"`
	Ts          int64    `bson:"ts,omitempty"`
	Error       string   `bson:"error,omitempty"`
}

type record struct {
	dstClient *mongo.Client
	srcClient *mongo.Client
	outDb     string
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
	if err := r.dstClient.Database(r.outDb).CreateCollection(context.Background(), diffCollection); err != nil {
		return err
	}
	_, err := r.dstClient.Database(r.outDb).Collection(diffCollection).Indexes().
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
	_, err = r.dstClient.Database(r.outDb).Collection(diffCollection).Indexes().
		CreateOne(context.Background(), im)
	return err
}

func (r *record) saveResult(result *compareResult) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := r.dstClient.Database(r.outDb).Collection(resultCollection).InsertOne(ctx, result)
	return err
}

func (r *record) clean() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return r.dstClient.Database(r.outDb).Drop(ctx)
}

func (r *record) saveMetaDiff(items []interface{}) error {
	return r.saveDiffBase(items, diffMetaCollection)
}

func (r *record) saveDiff(items []interface{}) error {
	return r.saveDiffBase(items, diffCollection)
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

	_, err := r.dstClient.Database(r.outDb).Collection(diffCollection).DeleteOne(ctx, bson.D{{"_id", id}})
	return err
}

func (r *record) saveStatus(status *taskStatus) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := r.dstClient.Database(r.outDb).Collection(statusCollection).FindOne(ctx, bson.D{{"_id", status.OId}}).
		Err()
	if err == mongo.ErrNoDocuments {
		if _, err := r.dstClient.Database(r.outDb).Collection(statusCollection).InsertOne(ctx, status); err != nil {
			return err
		}
		return nil
	}

	if err == nil {
		if _, err := r.dstClient.Database(r.outDb).Collection(statusCollection).ReplaceOne(ctx,
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

	_, err := r.dstClient.Database(r.outDb).Collection(statusCollection).
		UpdateOne(ctx, bson.D{{"_id", taskId}}, update)
	return err
}

func (r *record) updateStatus(taskId string, fields map[string]interface{}) error {
	update := bson.M{"$set": fields}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err := r.dstClient.Database(r.outDb).Collection(statusCollection).
		UpdateOne(ctx, bson.D{{"_id", taskId}}, update)
	return err
}

func (r *record) getStatus() (*taskStatus, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	res := r.dstClient.Database(r.outDb).Collection(statusCollection).FindOne(ctx, bson.D{},
		options.FindOne().SetMaxTime(timeout))
	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, res.Err()
	}
	var status taskStatus
	if err := res.Decode(&status); err != nil {
		return nil, err
	}
	return &status, nil
}

func (r *record) getDiffCount() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return r.dstClient.Database(r.outDb).Collection(diffCollection).EstimatedDocumentCount(ctx,
		options.EstimatedDocumentCount().SetMaxTime(timeout))
}

func (r *record) getSampleDiffData() ([][4]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout*3)
	defer cancel()

	pipeline := []bson.D{{{"$sample", bson.D{{"size", 10}}}}}
	cursor, err := r.dstClient.Database(r.outDb).Collection(diffCollection).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	res := make([][4]string, 0)
	for cursor.Next(ctx) {
		var di diffItem
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
		s, _ := json.Marshal(srcData)
		d, _ := json.Marshal(dstData)
		res = append(res, [4]string{di.Ns, fmt.Sprintf("%v", di.Id), string(s), string(d)})

	}
	return res, nil
}

func (r *record) getResult() ([]*compareResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cursor, err := r.dstClient.Database(r.outDb).Collection(resultCollection).Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	res := make([]*compareResult, 0)
	for cursor.Next(ctx) {
		var cr compareResult
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

func (r *record) getRecentTs() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	res := r.dstClient.Database(r.outDb).Collection(diffCollection).FindOne(ctx, bson.D{}, options.FindOne().
		SetSort(bson.D{{"ts", 1}}).SetMaxTime(timeout))
	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return -1, nil
		}
		return 0, res.Err()
	}
	var diff diffItem
	if err := res.Decode(&diff); err != nil {
		return 0, err
	}
	return int(diff.Ts), nil
}

func (r *record) getAccountMetaDiff() ([]*diffItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cursor, err := r.dstClient.Database(r.outDb).Collection(diffMetaCollection).Find(ctx, bson.D{},
		options.Find().SetMaxTime(timeout))
	if err != nil {
		return nil, err
	}
	res := make([]*diffItem, 0)
	for cursor.Next(ctx) {
		var di diffItem
		if err := cursor.Decode(&di); err != nil {
			return nil, err
		}
		if di.Ns == "admin.system.users" {
			res = append(res, &di)
		}
	}
	return res, nil
}
func (r *record) getJsMetaDiff() ([]*diffItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cursor, err := r.dstClient.Database(r.outDb).Collection(diffMetaCollection).Find(ctx, bson.D{},
		options.Find().SetMaxTime(timeout))
	if err != nil {
		return nil, err
	}
	res := make([]*diffItem, 0)
	for cursor.Next(ctx) {
		var di diffItem
		if err := cursor.Decode(&di); err != nil {
			return nil, err
		}
		if strings.Contains(di.Ns, ".system.js") {
			res = append(res, &di)
		}
	}
	return res, nil
}
func (r *record) getShardMetaDiff() ([]*diffItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cursor, err := r.dstClient.Database(r.outDb).Collection(diffMetaCollection).Find(ctx, bson.D{},
		options.Find().SetMaxTime(timeout))
	if err != nil {
		return nil, err
	}
	res := make([]*diffItem, 0)
	for cursor.Next(ctx) {
		var di diffItem
		if err := cursor.Decode(&di); err != nil {
			return nil, err
		}
		if di.Ns == "config.collections" {
			res = append(res, &di)
		}
	}
	return res, nil
}

type tagDoc struct {
	Ns  string
	Tag string
}

func (r *record) getTagsDiff() ([]*tagDoc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cursor, err := r.dstClient.Database(r.outDb).Collection(diffMetaCollection).Find(ctx, bson.D{},
		options.Find().SetMaxTime(timeout))
	if err != nil {
		return nil, err
	}
	res := make([]*tagDoc, 0)
	for cursor.Next(ctx) {
		var di diffItem
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
		var doc tagDoc
		doc.Ns, _ = srcData["ns"].(string)
		doc.Tag, _ = srcData["tag"].(string)
		res = append(res, &doc)
	}
	return res, nil
}

func (r *record) dropTable(tables ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for _, table := range tables {
		if err := r.dstClient.Database(r.outDb).Collection(table).Drop(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *record) setTaskError(taskId string, err error) error {
	return r.updateStatus(taskId, map[string]interface{}{"error": err.Error()})
}
