# Hadoop 大数据实验代码与数据

伪分布式 Hadoop 3.3.6（`/home/gray/hadoop-3.3.6`，启动脚本 `~/hadoop.sh`）、HBase 2.5.16、Spark 3.5.9 工作区：HDFS Java API、MapReduce 作业、HBase Shell/Java API、PageRank（MapReduce + Spark）、数据采集脚本与实验数据。

## 目录结构

```
hadoop-test/
├── webdata/               # 英文网页数据（13 篇，实验一 WordCount + 实验二倒排索引的输入源）
├── stopwords/
│   └── stopwords_en.txt   # 英文停词表（186 行，含撇号碎片补丁，实验二用）
├── scripts/
│   ├── fetch_pages.sh     # 通用网页抓取脚本（URL -> 干净的英文纯文本）
│   ├── urls.txt           # URL 清单（实验二 canonical 清单，已抓过的自动跳过）
│   ├── run_wordcount.sh   # 一键提交 WordCount 并查看词频 Top20（实验一）
│   ├── run_inverted_index.sh  # 一键提交倒排索引作业（实验二）
│   ├── run_hbase_demo.sh  # HBase Java API 演示（实验三）
│   ├── run_hbase_shell.sh # HBase Shell 5 类操作（实验三）
│   ├── run_pagerank_mr.sh # PageRank MapReduce 10 轮迭代（实验三）
│   └── run_pagerank_spark.sh  # PageRank Spark 10 轮迭代（实验三）
├── mapreduce/
│   └── src/main/java/sdu/bigdata/InvertedIndex.java  # 倒排索引实现（实验二）
├── hbase/
│   ├── src/main/java/sdu/bigdata/HBaseDemo.java      # HBase Java API（实验三）
│   └── shell/hbase_shell_tasks.hbase                 # HBase Shell 操作清单
├── pagerank/
│   ├── src/main/java/sdu/bigdata/PageRank.java       # PageRank MapReduce（实验三）
│   ├── src/main/java/sdu/bigdata/PageRankSpark.java  # PageRank Spark（实验三）
│   ├── scripts/prepare_web_graph.py                  # SNAP 边表 -> 邻接表
│   ├── scripts/compare_results.py                    # MR / Spark 结果一致性校验
│   └── data/sample-graph.txt                         # 1000 节点快速测试样本
├── output/                # HDFS 作业结果拉回本地的备份目录（手动用）
└── words.txt              # 早期实验用小词表
```

## 快速开始

```bash
# 0. 启动集群（若未启动）
bash ~/hadoop.sh start

# 1. 抓取网页数据（任选一种方式）
./scripts/fetch_pages.sh https://www.gnu.org/software/bash/          # 单个 URL
./scripts/fetch_pages.sh -n rust https://www.rust-lang.org/          # 指定文件名
./scripts/fetch_pages.sh -f scripts/urls.txt                         # 按清单批量抓取
./scripts/fetch_pages.sh --force -f scripts/urls.txt                 # 强制重抓

# 2. 数据同步到 HDFS（-f 覆盖同名旧文件，新文件自动加入）
hdfs dfs -put -f webdata/*.txt /input/webdata

# 3. 运行 WordCount（脚本会自动删旧输出、提交作业、显示 Top20）
./scripts/run_wordcount.sh                # 默认 /input/webdata -> /output
./scripts/run_wordcount.sh /input/webdata /output2   # 自定义输出目录

# 4. 查看结果
hdfs dfs -cat /output/part-r-00000 | head -30
hdfs dfs -cat /output/part-r-00000 | sort -t$'\t' -k2 -rn | head -20   # 词频排序
hdfs dfs -get /output/part-r-00000 output/                           # 拉回本地
```

## 实验二：文档倒排索引

源码见 `mapreduce/src/main/java/sdu/bigdata/InvertedIndex.java`。

```bash
# 1. 编译
bash mapreduce/doc/build.sh

# 2. 数据与停词表同步到 HDFS（数据已同步过可跳过）
hdfs dfs -put -f webdata/*.txt /input/webdata

# 3. 一键提交（停词表自动同步到 /input/stopwords.txt）
./scripts/run_inverted_index.sh       # /input/webdata -> /output/invindex

# 4. 查看
hdfs dfs -cat /output/invindex/part-r-00000 | head -30
```

## fetch_pages.sh 说明

- 自动判断内容形态：HTML 页面会去掉 `<script>/<style>/导航/页脚` 等只留正文；Markdown/纯文本页面做轻量清理（去代码块、链接语法、引用标记）。
- 输出文件名：默认由 URL 生成，`-n NAME` 可指定；清单文件里可在 URL 前写名字。
- 已存在的同名文件默认**跳过**，加 `--force` 覆盖。
- 正文太短（默认少于 200 个英文单词）或网络失败的页面会被丢弃并计入失败数。
- 依赖：`curl`、`python3`（仅标准库，无需安装任何包）。

## 实验三：HBase 操作 + PageRank（MapReduce / Spark）

环境：HBase 2.5.16（`/home/gray/hbase-2.5.16`，伪分布式，rootdir 为 `hdfs://localhost:19001/hbase`，自带 ZooKeeper）；Spark 3.5.9（`/home/gray/spark-3.5.9-bin-hadoop3`，local 模式）。

### HBase 部分

```bash
# 1. 启动（先启动 Hadoop，再启动 HBase）
bash ~/hadoop.sh start
/home/gray/hbase-2.5.16/bin/start-hbase.sh

# 2. 编译并运行 Java API 演示（建表/插入/按列浏览/修改/删除/清空/计数）
bash hbase/build.sh
bash scripts/run_hbase_demo.sh          # 结果: output/hbase_api_result.txt

# 3. HBase Shell 完成同样的 5 类操作
bash scripts/run_hbase_shell.sh         # 结果: output/hbase_shell_result.txt
```

### PageRank 部分

数据集用 SNAP Stanford Web Graph（281,903 个网页、2,312,497 条边，下载地址 https://snap.stanford.edu/data/web-Stanford.html）：

```bash
# 1. 下载并转换（边表 -> 网页\t出链1,出链2,...）
curl -LO https://snap.stanford.edu/data/web-Stanford.txt.gz
python3 pagerank/scripts/prepare_web_graph.py web-Stanford.txt.gz pagerank/data/web-graph.txt

# 2. 编译（同时生成 MapReduce 与 Spark 两个实现）
bash pagerank/build.sh

# 3. 10 轮迭代
bash scripts/run_pagerank_mr.sh          # /output/pagerank，约 7 分钟
bash scripts/run_pagerank_spark.sh       # /output/pagerank_spark，约 40 秒

# 4. 校验两份结果
python3 pagerank/scripts/compare_results.py \
  output/pagerank_mr_result.txt output/pagerank_spark_result.txt
```

快速验证可改用 1000 节点样本（`pagerank/data/sample-graph.txt`），把第 3 步命令的最后加两个参数即可：

```bash
bash scripts/run_pagerank_mr.sh pagerank/data/sample-graph.txt /output/pagerank_sample 2
bash scripts/run_pagerank_spark.sh pagerank/data/sample-graph.txt /output/pagerank_sample_spark 2
```

> **WSL 网络坑（Spark）**：本机 lo 上存在 `10.255.255.254`，Spark 的 `Utils.localHostName()` 会选它而非 `127.0.0.1`。必须设置 `SPARK_LOCAL_IP=10.255.255.254`，并加 `--conf spark.driver.host=10.255.255.254 --conf spark.driver.bindAddress=10.255.255.254`，否则 driver 的 Netty 服务会绑定在 10.255.255.254 而客户端连 127.0.0.1 被拒。`run_pagerank_spark.sh` 已封装。
>
> **HBase 坑**：HBase 2.5 默认的 AsyncFSWAL 在 Hadoop 3.3.6 上会报 `IllegalArgumentException: object is not an instance of declaring class`，需在 `hbase-site.xml` 设 `hbase.wal.provider=filesystem`；HBase 的 Web UI 为 http://localhost:16010。

## 注意事项

- **输出目录不能已存在**：重跑 WordCount 用 `run_wordcount.sh` 会自动删除；手动跑需先 `hdfs dfs -rm -r /output`。

## 故障排查

- **`localhost:9000 Connection refused`（端口明明在监听）**：本机 WSL 为 virtioproxy 网络模式，对回环端口的转发不稳定——两种症状：connect 被拒，或 TCP 能连但 RPC 间歇性 `EOFException`（成功率随机、时好时坏，重启集群无效）。解决：换 `fs.defaultFS` 端口规避，历史沿革 **9000 → 19000（2026-09 失效）→ 19001（现行）**；再失效继续换（19002…），Java API 代码里的端口要同步改。
- **NameNode 启动后 put 报 `Name node is in safe mode`**：等 DataNode 汇报完块自动退出（约半分钟），或手动 `hdfs dfsadmin -safemode leave`。
- **Wikipedia 等站点无法直连**（curl 超时）：本机网络环境限制；gnu.org、docs.python.org、kernel.org 等可正常抓取。
- **依赖 JS 渲染的站点抓不到正文**（如 go.dev）：脚本会自动丢弃过短页面，属预期行为。
- Web UI：YARN http://localhost:8088 ｜ HDFS http://localhost:9870 ｜ JobHistory http://localhost:19888 ｜ HBase Master http://localhost:16010
