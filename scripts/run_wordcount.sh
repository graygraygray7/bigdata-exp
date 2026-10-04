#!/usr/bin/env bash
# ============================================================
# run_wordcount.sh —— 一键运行 Hadoop 自带 WordCount
# 用法:
#   ./scripts/run_wordcount.sh                    # 默认 /input/webdata -> /output
#   ./scripts/run_wordcount.sh /input/webdata /output
# 可配合 fetch_pages.sh 使用:
#   hdfs dfs -put -f webdata/*.txt /input/webdata  # 先把新数据同步到 HDFS
# ============================================================
set -euo pipefail

export JAVA_HOME=/home/gray/java11
export HADOOP_HOME=/home/gray/hadoop-3.3.6
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

IN="${1:-/input/webdata}"
OUT="${2:-/output}"
JAR="$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar"

jps | grep -q NameNode || { echo "HDFS 未运行: bash ~/hadoop.sh start"; exit 1; }

hdfs dfs -rm -r -f "$OUT" >/dev/null 2>&1 || true

echo ">>> 提交 WordCount: $IN -> $OUT"
hadoop jar "$JAR" wordcount "$IN" "$OUT" 2>&1 | grep -E "completed successfully|map .* reduce|FAILED" || true

echo ""
echo ">>> 输出文件:"
hdfs dfs -ls "$OUT"

echo ""
echo ">>> 词频 Top 20:"
hdfs dfs -cat "$OUT/part-r-00000" | sort -t$'\t' -k2 -rn | head -20
