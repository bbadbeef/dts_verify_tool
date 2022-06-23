package main

import (
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"log"
	"mongo_compare/compare"
	"mongo_compare/httpserver"
	"mongo_compare/taskmanager"
	"mongo_compare/tools"
	"mongo_compare/utils"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	runMode        string
	srcUrl         string
	srcMongodUrl   string
	dstUrl         string
	verify         string
	resultDb       string
	specifiedDb    string
	specifiedNs    string
	srcConcurrency int
	dstConcurrency int
	sample         int
)

func init() {
	flag.StringVar(&runMode, "run_mode", "compare", `执行模式
- compare: 创建对比任务
- resume: 继续执行上一次任务
- show_config: 查看当前任务配置
- flush_config: 修改当前任务配置，当前只支持修改并发数
- show_task_info: 查看任务执行信息`)
	flag.StringVar(&srcUrl, "src_uri", "", `源数据库url
	示例 mongodb://mongouser:password@8.8.8.8:27017/?readPerference=secondaryPreferred`)
	flag.StringVar(&srcMongodUrl, "src_mongod_uri", "", "源端mongod url，每个分片用分号隔开。\n"+
		"示例 mongodb://mongouser:password@8.8.8.8:27017/?readPerference=secondaryPreferred;"+
		"mongodb://mongouser:password@9.9.9.9:27017/?readPerference=secondaryPreferred")
	flag.StringVar(&dstUrl, "dst_uri", "", `目的数据库url
	示例 mongodb://mongouser:password@8.8.8.8:27017/`)
	flag.StringVar(&verify, "verify", "", `校验类型
- account: 校验账号
- javascript: 校验存储过程
- shard_key: 校验分片键
- tag: 校验标签
- data_count_preview: 校验预估count数
- data_count: 校验准确count数
- data_all_content: 校验全量数据
- data_all: 校验全量+增量数据
- data_db_content: 校验指定db/collection全量数据
- data_db_all: 校验指定db/collection全量数据+增量数据
- all: 校验以上所有项`)
	flag.StringVar(&resultDb, "verify_result_db", "dts_verify_result", "结果存放db名")
	flag.StringVar(&specifiedDb, "specified_db", "", "需要校验的db，多个使用','分隔，"+
		"当 -verify 指定为 data_db_* 时生效")
	flag.StringVar(&specifiedNs, "specified_col", "", "需要校验的集合，多个使用','分隔，"+
		"当 -verify 指定为 data_db_* 时生效")
	flag.IntVar(&srcConcurrency, "src_concurrency", 0, "源端最大并发数，默认10")
	flag.IntVar(&dstConcurrency, "dst_concurrency", 0, "目的端最大并发数，默认100")
	flag.IntVar(&sample, "sample", 0, "抽样比例 (0,100),不指定表示取全量数据")
	flag.Parse()

	checkParam()

	needConfirm()

	handleSignal()
}

func handleSignal() {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGSEGV,
		syscall.SIGQUIT)
	go func() {
		for s := range c {
			switch s {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGSEGV:
				utils.GlobalLogger.Warnf("received os signal: %v, will exited", s)
				utils.GracefullyExit()
			default:
			}
		}
	}()
}

func decideByUser() {
	var action string
	fmt.Scanln(&action)
	a := strings.ToLower(action)
	if a == "yes" || a == "y" {
		return
	}
	os.Exit(0)
}

func needConfirm() {
	if runMode != "compare" {
		return
	}
	if !strings.Contains(srcUrl, "readPerference=secondary") ||
		(len(srcMongodUrl) != 0 && !strings.Contains(srcMongodUrl, "readPerference=secondary")) {
		fmt.Printf("not found read secondary perference, are you sure to read from primary? "+
			"%s to contine or %s(default) to abort\n", utils.Yellow("yes"), utils.Yellow("no"))
		decideByUser()
	}
	if verify == "data_count" {
		fmt.Printf("this task will count document by COLLSCAN, are you sure? "+
			"%s to contine or %s(default) to abort\n", utils.Yellow("yes"), utils.Yellow("no"))
		decideByUser()
	}
}

func checkUrl() {
	if len(srcUrl) == 0 || len(dstUrl) == 0 {
		fmt.Println("invalid argument, src_uri and dst_uri must be specified")
		flag.Usage()
		os.Exit(0)
	}
	if _, ok := compare.TaskStep[verify]; runMode == "compare" && !ok {
		fmt.Println("invalid argument, verify illegal")
		flag.Usage()
		os.Exit(0)
	}
	if verify == "data_db_content" || verify == "data_db_all" {
		if len(specifiedDb) == 0 && len(specifiedNs) == 0 {
			fmt.Println("invalid argument, db or ns need specified")
			flag.Usage()
			os.Exit(0)
		}
	}
}

func checkSample() {
	if sample < 0 || sample > 100 {
		fmt.Println("invalid argument, sample illegal")
		flag.Usage()
		os.Exit(0)
	}
	if sample > 0 {
		if verify == "data_all" || verify == "data_db_all" || verify == "all" {
			fmt.Println("invalid argument, sample compare not support this compare type")
			flag.Usage()
			os.Exit(0)
		}
	}
}

func checkParam() {
	switch runMode {
	case "compare", "resume":
		checkUrl()
		if srcConcurrency == 0 {
			srcConcurrency = 10
		}
		if dstConcurrency == 0 {
			dstConcurrency = 100
		}
		if srcConcurrency == 0 || dstConcurrency == 0 {
			fmt.Println("invalid argument, concurrency illegal")
			flag.Usage()
			os.Exit(0)
		}
		checkSample()
	case "show_config":
	case "flush_config":
		if srcConcurrency == 0 && dstConcurrency == 0 {
			fmt.Println("invalid argument")
			flag.Usage()
			os.Exit(0)
		}
	case "server":
	case "show_task_info":
	default:
		fmt.Println("invalid argument, unknown run mode")
		flag.Usage()
		os.Exit(0)
	}
}

func initLog() *logrus.Logger {
	w, err := utils.GetRotateWriter()
	if err != nil {
		log.Panic(err)
	}
	return utils.NewLogger(w, "")
}

func main() {
	l := initLog()
	utils.SetGlobalLogger(l)

	utils.InitConfig()

	switch runMode {
	case "show_config":
		tools.ShowConfig()
	case "flush_config":
		tools.SetConfig(srcConcurrency, dstConcurrency)
	case "show_task_info":
		if len(srcUrl) == 0 || len(dstUrl) == 0 {
			tools.ShowTaskInfo()
		} else {
			para := &compare.Parameter{
				SrcUrl:   srcUrl,
				DstUrl:   dstUrl,
				ResultDb: resultDb,
			}
			info := compare.DisplayTaskStatus(para)
			os.Stdout.WriteString(info)
		}
		os.Exit(0)
	case "server":
		fmt.Println("not implement")
		os.Exit(0)
	case "compare", "resume":
		l.Infof("tool start, mode: %s ...", runMode)
		httpserver.Start()

		para := pacPara()
		task := compare.NewTask(para)
		if err := task.Init(); err != nil {
			l.Error("run init error: ", err.Error())
			fmt.Println("init task error: ", err.Error())
			return
		}
		taskmanager.AddTask(task)
		if err := task.Run(); err != nil {
			l.Error("run task error: ", err.Error())
			fmt.Println("run task error: ", err.Error())
			task.SetStatus(compare.StatusFailed)
			return
		}
		task.SetStatus(compare.StatusSuccess)
		fmt.Println("task success, now exit")
		os.Stdout.WriteString(task.Display())
	}
}

func pacPara() *compare.Parameter {
	return &compare.Parameter{
		SrcUrl: srcUrl,
		SrcMongodUrl: func() []string {
			res := make([]string, 0)
			uris := strings.Split(srcMongodUrl, ";")
			for _, uri := range uris {
				if len(uri) != 0 {
					res = append(res, uri)
				}
			}
			return res
		}(),
		DstUrl:      dstUrl,
		CompareType: verify,
		SpecifiedDb: func() []string {
			res := make([]string, 0)
			dbs := strings.Split(specifiedDb, ",")
			for _, db := range dbs {
				if len(db) != 0 {
					res = append(res, db)
				}
			}
			return res
		}(),
		SpecifiedNs: func() []string {
			res := make([]string, 0)
			nss := strings.Split(specifiedNs, ",")
			for _, ns := range nss {
				if len(ns) != 0 {
					res = append(res, ns)
				}
			}
			return res
		}(),
		ResultDb:       resultDb,
		SrcConcurrency: srcConcurrency,
		DstConcurrency: dstConcurrency,
		RunMode:        runMode,
		Sample:         sample,
	}
}
