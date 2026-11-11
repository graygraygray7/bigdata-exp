#!/usr/bin/env bash
# ============================================================
# run_pagerank_mr.sh —— 一键运行 PageRank 的 MapReduce 实现
#
# 用法:
#   ./scripts/run_pagerank_mr.sh                                   # 全图 10 轮 -> /output/pagerank
#   ./scripts/run_pagerank_mr.sh pagerank/data/sample-graph.txt /output/pagerank_sample 2
#
# 参数: [本地输入文件] [HDFS 输出目录] [迭代次数]
# ============================================================
set -euo pipefail
cd "$(dirname "$0")/.."

export JAVA_HOME=/home/gray/java11
export HADOOP_HOME=/home/gray/hadoop-3.3.6
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

LOCAL_IN="${1:-pagerank/data/web-graph.txt}"
OUT="${2:-/output/pagerank}"
ITER="${3:-10}"
HDFS_IN="/input/pagerank/$(basename "$LOCAL_IN")"
JAR="pagerank/pagerank-1.0.jar"

[[ -f $JAR ]] || { echo "未找到 $JAR，先执行: bash pagerank/build.sh"; exit 1; }
[[ -f $LOCAL_IN ]] || { echo "未找到输入文件: $LOCAL_IN"; exit 1; }
jps | grep -q NameNode || { echo "HDFS 未运行: bash ~/hadoop.sh start"; exit 1; }

echo ">>> 上传邻接表: $LOCAL_IN -> $HDFS_IN"
hdfs dfs -mkdir -p "$(dirname "$HDFS_IN")"
hdfs dfs -put -f "$LOCAL_IN" "$HDFS_IN"

echo ">>> 提交 PageRank MapReduce: $HDFS_IN -> $OUT（$ITER 轮）"
hdfs dfs -rm -r -f "$OUT" >/dev/null 2>&1 || true
hadoop jar "$JAR" sdu.bigdata.PageRank "$HDFS_IN" "$OUT" "$ITER" 2>&1 \
  | grep -E '>>>|completed successfully|map [0-9]+%|reduce [0-9]+%|✅|失败|Exception' || true

echo ""
echo ">>> 输出文件:"
hdfs dfs -ls "$OUT/result"

echo ""
echo ">>> Top 20:"
hdfs dfs -cat "$OUT/result/part-r-00000" 2>/dev/null | head -20 || true

mkdir -p output
NAME="$(basename "$OUT")"
if hdfs dfs -test -e "$OUT/result/part-r-00000"; then
  hdfs dfs -get -f "$OUT/result/part-r-00000" "output/${NAME}_mr_result.txt" 2>/dev/null || true
  if [[ -f "output/${NAME}_mr_result.txt" ]]; then
    echo ""
    echo ">>> 已拉回本地: output/${NAME}_mr_result.txt"
  fi
fi
