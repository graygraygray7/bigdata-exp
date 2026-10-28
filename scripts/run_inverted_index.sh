#!/usr/bin/env bash
# ============================================================
# run_inverted_index.sh —— 一键提交倒排索引作业（实验二）
#
# 用法:
#   ./scripts/run_inverted_index.sh                                  # 默认 /input/webdata -> /output/invindex
#   ./scripts/run_inverted_index.sh /input/webdata /output/invidx2   # 自定义输出目录
#
# 前置条件:
#   1. InvertedIndex.java 的 TODO①~⑦ 已全部填写
#   2. 已编译打包:  bash mapreduce/doc/build.sh   产物 mapreduce/mr-exp-1.0.jar
#   3. 数据已在 HDFS:  hdfs dfs -put -f webdata/*.txt /input/webdata
#      (停词表本脚本会自动同步到 /input/stopwords.txt，代码通过 addCacheFile 读取)
# ============================================================
set -euo pipefail
cd "$(dirname "$0")/.."

export JAVA_HOME=/home/gray/java11
export HADOOP_HOME=/home/gray/hadoop-3.3.6
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

IN="${1:-/input/webdata}"
OUT="${2:-/output/invindex}"
JAR="mapreduce/mr-exp-1.0.jar"

[[ -f $JAR ]] || { echo "未找到 $JAR，先填完 TODO①~⑦ 再执行: bash mapreduce/doc/build.sh"; exit 1; }
jps | grep -q NameNode || { echo "HDFS 未运行: bash ~/hadoop.sh start"; exit 1; }

# 停词表自动同步到 HDFS（实验要求"建立停词表"，此文件会被分发到每个 map 任务）
hdfs dfs -put -f stopwords/stopwords_en.txt /input/stopwords.txt

# 输出目录不能已存在（MapReduce 约定）
hdfs dfs -rm -r -f "$OUT" >/dev/null 2>&1 || true

echo ">>> 提交倒排索引: $IN -> $OUT"
hadoop jar "$JAR" sdu.bigdata.InvertedIndex "$IN" "$OUT" 2>&1 \
  | grep -E "completed successfully|map .* reduce|FAILED" || true

echo ""
echo ">>> 输出文件:"
hdfs dfs -ls "$OUT"

echo ""
echo ">>> 结果预览（前 20 行）:"
hdfs dfs -cat "$OUT/part-r-00000" | grep -a $'\t <' | head -20 || true

echo ""
echo ">>> 顺手抽查几个跨文档词:"
for w in python software index hadoop; do
  hdfs dfs -cat "$OUT/part-r-00000" | awk -v w=$w '$1==w' | head -1
done

echo ""
echo ">>> 统计:"
RES=$(hdfs dfs -cat "$OUT/part-r-00000" | grep -a $'\t <')
RES_NUM=$(echo "$RES" | wc -l)
MULTI=$(echo "$RES" | awk -F';' 'NF>3' | wc -l)
echo "  词表共 $RES_NUM 个单词，其中同时出现在 3 篇以上文档的 $MULTI 个"
RES_FILE=${OUT##*/}
hdfs dfs -get "$OUT/part-r-00000" "output/${RES_FILE}.part-r-00000" 2>/dev/null && \
  echo "  已拉回本地备份: output/${RES_FILE}.part-r-00000" || true
