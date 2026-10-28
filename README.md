# Hadoop 大数据实验代码与数据

伪分布式 Hadoop 3.3.6（`/home/gray/hadoop-3.3.6`，启动脚本 `~/hadoop.sh`）工作区：HDFS Java API、MapReduce 作业、数据采集脚本与实验数据。

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
│   └── run_inverted_index.sh  # 一键提交倒排索引作业（实验二）
├── mapreduce/
│   └── src/main/java/sdu/bigdata/InvertedIndex.java  # 倒排索引实现（实验二）
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

## 注意事项

- **输出目录不能已存在**：重跑 WordCount 用 `run_wordcount.sh` 会自动删除；手动跑需先 `hdfs dfs -rm -r /output`。

## 故障排查

- **`localhost:9000 Connection refused`（端口明明在监听）**：本机 WSL 为 virtioproxy 网络模式，对回环端口的转发不稳定——两种症状：connect 被拒，或 TCP 能连但 RPC 间歇性 `EOFException`（成功率随机、时好时坏，重启集群无效）。解决：换 `fs.defaultFS` 端口规避，历史沿革 **9000 → 19000（2026-09 失效）→ 19001（现行）**；再失效继续换（19002…），Java API 代码里的端口要同步改。
- **NameNode 启动后 put 报 `Name node is in safe mode`**：等 DataNode 汇报完块自动退出（约半分钟），或手动 `hdfs dfsadmin -safemode leave`。
- **Wikipedia 等站点无法直连**（curl 超时）：本机网络环境限制；gnu.org、docs.python.org、kernel.org 等可正常抓取。
- **依赖 JS 渲染的站点抓不到正文**（如 go.dev）：脚本会自动丢弃过短页面，属预期行为。
- Web UI：YARN http://localhost:8088 ｜ HDFS http://localhost:9870 ｜ JobHistory http://localhost:19888
