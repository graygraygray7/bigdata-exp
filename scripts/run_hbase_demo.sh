#!/usr/bin/env bash
# ============================================================
# run_hbase_demo.sh —— 运行实验三第一部分的 HBase Java API 演示
#
# 用法:
#   bash scripts/run_hbase_demo.sh
#
# 前置条件:
#   1. Hadoop 与 HBase 已启动:  bash ~/hadoop.sh start && ~/hbase-2.5.16/bin/start-hbase.sh
#   2. 已编译:                  bash hbase/build.sh
# ============================================================
set -euo pipefail
cd "$(dirname "$0")/.."

export JAVA_HOME=/home/gray/java11
export HADOOP_HOME=/home/gray/hadoop-3.3.6
export HBASE_HOME=/home/gray/hbase-2.5.16
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HBASE_HOME/bin:$PATH"

JAR="hbase/hbase-demo-1.0.jar"
[[ -f $JAR ]] || { echo "未找到 $JAR，先执行: bash hbase/build.sh"; exit 1; }

jps | grep -q HMaster || { echo "HBase 未运行: $HBASE_HOME/bin/start-hbase.sh"; exit 1; }

CP="$("$HBASE_HOME/bin/hbase" classpath 2>/dev/null)"

echo ">>> 运行 HBaseDemo（结果同时保存到 output/hbase_api_result.txt）"
mkdir -p output
"$JAVA_HOME/bin/java" -Djava.net.preferIPv4Stack=true -Dhbase.root.logger=WARN,console \
  -cp "$CP:$JAR" sdu.bigdata.HBaseDemo 2>&1 \
  | tee output/hbase_api_result.txt
