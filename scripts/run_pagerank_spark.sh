#!/usr/bin/env bash
# ============================================================
# run_pagerank_spark.sh —— 一键运行 PageRank 的 Spark 实现
#
# 用法:
#   ./scripts/run_pagerank_spark.sh                                   # 全图 10 轮
#   ./scripts/run_pagerank_spark.sh pagerank/data/sample-graph.txt /output/pagerank_sample_spark 2
#
# 参数: [本地输入文件] [HDFS 输出目录] [迭代次数]
#
# 注意：本机 WSL 的 lo 上有 10.255.255.254，Spark 的 Utils.localHostName()
# 会选中它而非 127.0.0.1；因此显式设置 SPARK_LOCAL_IP / driver.host 为该地址，
# 否则 driver 的 Netty 服务会绑定在 10.255.255.254，而客户端去连 127.0.0.1 被拒。
# ============================================================
set -euo pipefail
cd "$(dirname "$0")/.."

export JAVA_HOME=/home/gray/java11
export HADOOP_HOME=/home/gray/hadoop-3.3.6
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME=/home/gray/spark-3.5.9-bin-hadoop3
export SPARK_LOCAL_IP=10.255.255.254
export JAVA_TOOL_OPTIONS="-Djava.net.preferIPv4Stack=true"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH"

LOCAL_IN="${1:-pagerank/data/web-graph.txt}"
OUT="${2:-/output/pagerank_spark}"
ITER="${3:-10}"
HDFS_IN="/input/pagerank/$(basename "$LOCAL_IN")"
JAR="pagerank/pagerank-1.0.jar"

[[ -f $JAR ]] || { echo "未找到 $JAR，先执行: bash pagerank/build.sh"; exit 1; }
[[ -f $LOCAL_IN ]] || { echo "未找到输入文件: $LOCAL_IN"; exit 1; }
jps | grep -q NameNode || { echo "HDFS 未运行: bash ~/hadoop.sh start"; exit 1; }

echo ">>> 上传邻接表: $LOCAL_IN -> $HDFS_IN"
hdfs dfs -mkdir -p "$(dirname "$HDFS_IN")"
hdfs dfs -put -f "$LOCAL_IN" "$HDFS_IN"

echo ">>> 提交 PageRank Spark: $HDFS_IN -> $OUT（$ITER 轮）"
hdfs dfs -rm -r -f "$OUT" >/dev/null 2>&1 || true
JAVA_TOOL_OPTIONS="-Djava.net.preferIPv4Stack=true" "$SPARK_HOME/bin/spark-submit" \
  --master 'local[4]' \
  --driver-memory 2g \
  --conf spark.driver.host=10.255.255.254 \
  --conf spark.driver.bindAddress=10.255.255.254 \
  --conf spark.ui.port=4041 \
  --class sdu.bigdata.PageRankSpark \
  "$JAR" "$HDFS_IN" "$OUT" "$ITER" 2>&1 \
  | grep -E '>>>|✅|Error|Exception|Traceback' || true

echo ""
echo ">>> 输出文件:"
hdfs dfs -ls "$OUT"

echo ""
echo ">>> Top 20:"
hdfs dfs -cat "$OUT/part-00000" 2>/dev/null | head -20 || true

mkdir -p output
NAME="$(basename "$OUT")"
if hdfs dfs -test -e "$OUT/part-00000"; then
  hdfs dfs -get -f "$OUT/part-00000" "output/${NAME}_result.txt" 2>/dev/null || true
  if [[ -f "output/${NAME}_result.txt" ]]; then
    echo ""
    echo ">>> 已拉回本地: output/${NAME}_result.txt"
  fi
fi
