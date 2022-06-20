# dts_verify_tool
dts_verify_tool是一个数据迁移校验工具，用来对比数据迁移的源端和目的端的数据，支持以下校验项

|校验项|校验内容|参数|
|:---|:---|:---|
|账号|账号名和角色|account|
|存储过程|存储过程内容|javascript|
|片键|片键内容|shard_key|
|分片集群tag|分片tag|tag|
|数据|预估count数|data_count_preview|
|数据|准确count数|data_count|
|数据|全量数据|data_all_content|
|数据|全量+增量数据|data_all|
|数据|指定DB/Collection全量数据|data_db_content|
|数据|指定DB/Collection全量+增量数据|data_db_all|
|全部|以上全部内容|all|

### 使用方式
```
??????? $ ./dts_verify_tool -h
Usage of ./dts_verify_tool:
-dst_concurrency int
    目的端最大并发数，默认100
-dst_uri string
    目的数据库url
    示例 mongodb://mongouser:password@8.8.8.8:27017/
-run_mode string
    执行模式
    - compare: 创建对比任务
    - resume: 继续执行上一次任务
    - show_config: 查看当前任务配置
    - flush_config: 修改当前任务配置，当前只支持修改并发数
    - show_task_info: 查看任务执行信息 (default "compare")
-sample int
    抽样比例 (0,100),不指定表示取全量数据
-specified_db string
    需要校验的db，多个使用','分隔
-specified_col string
    需要校验的集合，多个使用','分隔
-src_concurrency int
    源端最大并发数，默认10
-src_mongod_uri string
    源端mongod url，每个分片用分号隔开。
    示例 mongodb://mongouser:password@8.8.8.8:27017/?readPerference=secondaryPreferred;mongodb://mongouser:password@9.9.9.9:27017/?readPerference=secondaryPreferred
-src_uri string
    源数据库url
    示例 mongodb://mongouser:password@8.8.8.8:27017/?readPerference=secondaryPreferred
-verify string
    校验类型
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
    - all: 校验以上所有项
-verify_result_db string
    结果存放db名 (default "dts_verify_result")
```

1. 创建对比任务，./dts_verify_tool -verify=data_db_all -src_uri="xxx" -dst_uri="xxx"
2. 创建后任务开始执行，此时可以执行 ./dts_verify_tool -run_mode=show_task_info 查看任务执行详情
3. 如果要查看任务参数，执行 ./dts_verify_tool -run_mode=show_config
4. 如果要调整并发数，执行 ./dts_verify_tool -run_mode=flush_config -src_concurrenct=20 -dst_concurrency=500 进行调整
5. 如果任务在执行中手动终止或异常退出，需要继续之前的任务，执行 ./dts_verify_tool -run_mode=resume -src_uri="xxx" -dst_uri="xxx"
6. 任务结束后，如果要查看任务信息，执行 ./dts_verify_tool -run_mode=show_task_info -src_uri="xxx" -dst_uri="xxx"。注意此时需要带上源端和目的端的连接串参数

### 注意事项
+ 校验任务应该在数据传输服务的全量数据同步完成后开始
+ 分片集群如果做增量校验，需要传入mongod的地址（-src_mongod_uri参数），否则会报拿不到oplog的错误
+ 校验data_count时会做全表扫描，可能会影响集群的性能
+ 源端连接串建议带上readPerference=secondaryPreferred参数
