package compare

import (
	"bytes"
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"mongo_compare/utils"
	"time"
)

// DisplayTaskStatus ...
func DisplayTaskStatus(p *Parameter) string {
	srcClient, err := newMongoClient(p.SrcUrl)
	if err != nil {
		return utils.Red(fmt.Sprintf("ERROR: %s\n", err.Error()))
	}
	dstClient, err := newMongoClient(p.DstUrl)
	if err != nil {
		return utils.Red(fmt.Sprintf("ERROR: %s\n", err.Error()))
	}
	r := &record{srcClient: srcClient, dstClient: dstClient, outDb: p.ResultDb}
	return getTaskStatusByRecord(r)
}

func getTaskStatusByRecord(r *record) string {
	status, err := r.getStatus()
	if err != nil {
		return utils.Red(fmt.Sprintf("ERROR: %s\n", err.Error()))
	}
	if status == nil {
		return utils.Red("task not found\n")
	}
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("%-20s %s\n", "status:", consoleStatus[status.Status]))
	b.WriteString(fmt.Sprintf("%-20s %s\n", "verify type", status.Name))
	b.WriteString(fmt.Sprintf("%-20s %s\n", "current step:", status.Step))

	if len(status.Error) != 0 {
		b.WriteString(fmt.Sprintf("%-20s %s\n", "error:", status.Error))
	}

	switch status.Name {
	case "data_count_preview", "data_count":
		displayCount(r, &b)
	case "account":
		displayAccount(r, &b)
	case "shard_key":
		displayShard(r, &b)
	case "tag":
		displayTag(r, &b)
	case "javascript":
		displayJs(r, &b)
	case "data_all_content", "data_all", "data_db_content", "data_db_all":
		displayData(r, &b, status)
	case "all":
		displayAccount(r, &b)
		displayShard(r, &b)
		displayTag(r, &b)
		displayJs(r, &b)
		displayData(r, &b, status)
	default:
		break
	}

	return b.String()
}

func displayCount(r *record, b *bytes.Buffer) {
	results, err := r.getResult()
	if err != nil {
		b.WriteString(fmt.Sprintf("%-20s %s\n", "count compare: ", err.Error()))
		return
	}
	for _, result := range results {
		if result.Step == "countCompareJob" {
			if result.Identical == "yes" {
				b.WriteString(fmt.Sprintf("%-20s %s\n", "count compare: ", "equal"))
				return
			}
			b.WriteString(fmt.Sprintf("%-20s %s\n", "count compare: ", "not equal"))
			b.WriteString(fmt.Sprintf("%s:\n", "diff"))
			m, ok := result.Diff.(primitive.D)
			if !ok {
				return
			}
			src, ok := m.Map()["source"].(primitive.D)
			if !ok {
				fmt.Println("2 error")
				return
			}
			b.WriteString(fmt.Sprintf("\t%s:\n", "source"))
			for k, v := range src.Map() {
				b.WriteString(fmt.Sprintf("\t\t%-20s %d\n", k+":", v))
			}
			dst, ok := m.Map()["destination"].(primitive.D)
			if !ok {
				return
			}
			b.WriteString(fmt.Sprintf("\t%s:\n", "destination"))
			for k, v := range dst.Map() {
				b.WriteString(fmt.Sprintf("\t\t%-20s %d\n", k+":", v))
			}
			return
		}
	}
}

func displayAccount(r *record, b *bytes.Buffer) {
	results, err := r.getResult()
	if err != nil {
		b.WriteString(fmt.Sprintf("%-20s %s\n", "account compare: ", err.Error()))
		return
	}
	for _, result := range results {
		if result.Step == "accountDataJob" {
			if result.Identical == "yes" {
				b.WriteString(fmt.Sprintf("%-20s %s\n", "account compare: ", "equal"))
				return
			}
			b.WriteString(fmt.Sprintf("%-20s %s\n", "account compare: ", "not equal"))
			b.WriteString(fmt.Sprintf("\t%s:\n", "account diff"))

			items, err := r.getAccountMetaDiff()
			if err != nil {
				b.WriteString(fmt.Sprintf("\t%s\n", err.Error()))
				return
			}
			for _, item := range items {
				b.WriteString(fmt.Sprintf("\t\t%-20s %-20s\n", fmt.Sprintf("namespace: %s", item.Ns),
					fmt.Sprintf("account: %v", item.Id)))
			}
			return
		}
	}
}

func displayShard(r *record, b *bytes.Buffer) {
	results, err := r.getResult()
	if err != nil {
		b.WriteString(fmt.Sprintf("%-20s %s\n", "shard key compare: ", err.Error()))
		return
	}
	for _, result := range results {
		if result.Step == "shardKeyDataJob" {
			if result.Identical == "yes" {
				b.WriteString(fmt.Sprintf("%-20s %s\n", "shard key compare: ", "equal"))
				return
			}
			b.WriteString(fmt.Sprintf("%-20s %s\n", "shard key compare: ", "not equal"))
			b.WriteString(fmt.Sprintf("\t%s:\n", "shard key diff"))

			items, err := r.getShardMetaDiff()
			if err != nil {
				b.WriteString(fmt.Sprintf("\t%s\n", err.Error()))
				return
			}
			for _, item := range items {
				b.WriteString(fmt.Sprintf("\t\t%-20s\n", fmt.Sprintf("shard collection: %v", item.Id)))
			}
			return
		}
	}
}

func displayJs(r *record, b *bytes.Buffer) {
	results, err := r.getResult()
	if err != nil {
		b.WriteString(fmt.Sprintf("%-20s %s\n", "javascript compare: ", err.Error()))
		return
	}
	for _, result := range results {
		if result.Step == "javascriptDataJob" {
			if result.Identical == "yes" {
				b.WriteString(fmt.Sprintf("%-20s %s\n", "javascript compare: ", "equal"))
				return
			}
			b.WriteString(fmt.Sprintf("%-20s %s\n", "javascript compare: ", "not equal"))
			b.WriteString(fmt.Sprintf("\t%s:\n", "javascript diff"))

			items, err := r.getJsMetaDiff()
			if err != nil {
				b.WriteString(fmt.Sprintf("\t%s\n", err.Error()))
				return
			}
			for _, item := range items {
				b.WriteString(fmt.Sprintf("\t\t%-20s %-20s\n", fmt.Sprintf("namespace: %s", item.Ns),
					fmt.Sprintf("javascript function: %v", item.Id)))
			}
			return
		}
	}
}

func displayTag(r *record, b *bytes.Buffer) {
	results, err := r.getResult()
	if err != nil {
		b.WriteString(fmt.Sprintf("%-20s %s\n", "tags compare: ", err.Error()))
		return
	}
	for _, result := range results {
		if result.Step == "tagDataJob" {
			if result.Identical == "yes" {
				b.WriteString(fmt.Sprintf("%-20s %s\n", "tags compare: ", "equal"))
				return
			}
			b.WriteString(fmt.Sprintf("%-20s %s\n", "tags compare: ", "not equal"))
			b.WriteString(fmt.Sprintf("\t%s:\n", "tags diff"))

			items, err := r.getTagsDiff()
			if err != nil {
				b.WriteString(fmt.Sprintf("\t%s\n", err.Error()))
				return
			}
			for _, item := range items {
				b.WriteString(fmt.Sprintf("\t\t%-20s %-20s\n", fmt.Sprintf("collection: %s", item.Ns),
					fmt.Sprintf("tag: %v", item.Tag)))
			}
			return
		}
	}
}

func displayData(r *record, b *bytes.Buffer, status *taskStatus) {
	if status.Step == "staticDataJob" {
		b.WriteString(fmt.Sprintf("%-20s %d\n", "progress(%):", status.Progress))
		b.WriteString(fmt.Sprintf("%-20s %s\n", "finished/total(namespace):", status.FinishNsCnt))
	}

	for {
		if status.Step == "dynamicDataJob" {
			ts, err := r.getRecentTs()
			if err != nil {
				b.WriteString(fmt.Sprintf("%-20s %s\n", "sync ts:", err.Error()))
				break
			}
			b.WriteString(fmt.Sprintf("%-20s %d\n", "sync ts:", ts))
			if ts > 0 {
				b.WriteString(fmt.Sprintf("%-20s %d\n", "delay second:", int(time.Now().Unix())-ts))
			}
		}
		break
	}

	if status.Step == "dynamicDataJob" || status.Step == "staticDataJob" {
		var cnt int64
		var err error
		b.WriteString(fmt.Sprintf("%-20s %v\n", "diff count:", func() interface{} {
			if cnt, err = r.getDiffCount(); err != nil {
				return utils.Red("ERROR")
			} else {
				return cnt
			}
		}()))
		if cnt != 0 {
			b.WriteString("diff data sample (10): \n")
			diff, err := r.getSampleDiffData()
			if err != nil {
				b.WriteString("\t" + err.Error() + "\n")
			}
			for _, d := range diff {
				b.WriteString(utils.Blue(fmt.Sprintf("\t%-20s %s\n\t\tsrc: %v\n\t\tdst: %v\n\n",
					"ns: "+d[0], "id: "+d[1], d[2], d[3])))
			}
		}
	}
}
