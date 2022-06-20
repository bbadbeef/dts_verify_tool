package compare

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mongo_compare/utils"
	"os"
)

// Parameter ...
type Parameter struct {
	SrcUrl         string   `json:"src_uri"`
	SrcMongodUrl   []string `json:"src_mongod_uri"`
	DstUrl         string   `json:"dst_uri"`
	CompareType    string   `json:"verify"`
	SpecifiedDb    []string `json:"specified_db,omitempty"`
	SpecifiedNs    []string `json:"specified_ns,omitempty"`
	SrcConcurrency int      `json:"src_concurrency"`
	DstConcurrency int      `json:"dst_concurrency"`
	ResultDb       string   `json:"verify_result_db"`
	Sample         int      `json:"sample,omitempty"`
	RunMode        string   `json:"-"`
	output         *os.File
	dirty          bool
}

// Marshal ...
func (p *Parameter) Marshal() []byte {
	if res, err := json.Marshal(p); err != nil {
		return nil
	} else {
		return res
	}
}

func (p *Parameter) display() string {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("%-20s %s\n", "src_uri:", p.SrcUrl))
	b.WriteString(fmt.Sprintf("%-20s %s\n", "dst_uri:", p.DstUrl))
	b.WriteString(fmt.Sprintf("%-20s %s\n", "verify:", p.CompareType))
	b.WriteString(fmt.Sprintf("%-20s %d\n", "src_concurrency:", p.SrcConcurrency))
	b.WriteString(fmt.Sprintf("%-20s %d\n", "dst_concurrency:", p.DstConcurrency))
	b.WriteString(fmt.Sprintf("%-20s %s\n", "verify_result_db:", p.ResultDb))
	if len(p.SpecifiedDb) != 0 {
		b.WriteString(fmt.Sprintf("%-20s %v\n", "specified_db:", p.SpecifiedDb))
	}
	if len(p.SpecifiedNs) != 0 {
		b.WriteString(fmt.Sprintf("%-20s %v\n", "specified_ns:", p.SpecifiedNs))
	}
	if p.Sample != 0 {
		b.WriteString(fmt.Sprintf("%-20s %d\n", "sample:", p.Sample))
	}
	return utils.Yellow(b.String())
}
