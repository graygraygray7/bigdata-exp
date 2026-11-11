#!/usr/bin/env bash
# ============================================================
# run_hbase_shell.sh —— 用 HBase Shell 完成实验三第一部分的 5 类操作
# 输出保存到 output/hbase_shell_result.txt
# ============================================================
set -euo pipefail
cd "$(dirname "$0")/.."

export JAVA_HOME=/home/gray/java11
export HADOOP_HOME=/home/gray/hadoop-3.3.6
export HBASE_HOME=/home/gray/hbase-2.5.16
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HBASE_HOME/bin:$PATH"

jps | grep -q HMaster || { echo "HBase 未运行: $HBASE_HOME/bin/start-hbase.sh"; exit 1; }

mkdir -p output
echo ">>> 执行 HBase Shell 任务（结果保存到 output/hbase_shell_result.txt）"
"$HBASE_HOME/bin/hbase" shell < hbase/shell/hbase_shell_tasks.hbase 2>&1 \
  | tee output/hbase_shell_result.txt
