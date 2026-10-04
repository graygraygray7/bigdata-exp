#!/usr/bin/env bash
# MapReduce 三题一键演示：编译 → 造数据 → 上传 → 依次提交作业 → 看输出
# 用法: bash doc/run_all.sh   （输出目录先删后建，可重复运行）
set -euo pipefail
cd "$(dirname "$0")/.."

export PATH="/home/gray/java11/bin:/home/gray/hadoop-3.3.6/bin:$PATH"
[ -f mr-exp-1.0.jar ] || bash doc/build.sh
JAR=mr-exp-1.0.jar

mkdir -p /tmp/mr_exp/sort /tmp/mr_exp/family
printf '20170101\tx\n20170102\ty\n20170103\tx\n20170104\ty\n20170105\tz\n20170106\tx\n' > /tmp/mr_exp/A.txt
printf '20170101\ty\n20170102\ty\n20170103\tx\n20170104\tz\n20170105\ty\n'          > /tmp/mr_exp/B.txt
printf '33\n37\n12\n40\n' > /tmp/mr_exp/sort/f1.txt; printf '4\n16\n39\n5\n' > /tmp/mr_exp/sort/f2.txt; printf '1\n45\n25\n' > /tmp/mr_exp/sort/f3.txt
cat > /tmp/mr_exp/family/child-parent.txt <<'EOF'
child	parent
Steven	Lucy
Steven	Jack
Jone	Lucy
Jone	Jack
Lucy	Mary
Lucy	Frank
Jack	Alice
Jack	Jesse
David	Alice
David	Jesse
Philip	David
Philip	Alma
Mark	David
Mark	Alma
EOF

hdfs dfs -mkdir -p /input/merge /input/sort /input/family
hdfs dfs -put -f /tmp/mr_exp/A.txt /tmp/mr_exp/B.txt /input/merge
hdfs dfs -put -f /tmp/mr_exp/sort/f1.txt /tmp/mr_exp/sort/f2.txt /tmp/mr_exp/sort/f3.txt /input/sort
hdfs dfs -put -f /tmp/mr_exp/family/child-parent.txt /input/family
hdfs dfs -rm -r -f /output/merge /output/sort /output/family

run(){ echo; echo ">>> $1"; shift; hadoop jar $JAR sdu.bigdata."$@" 2>&1 | grep -E 'completed successfully|FAILED|map 100% reduce 100%'; hdfs dfs -cat "$3"/part-r-00000; }

run "3-1 合并去重" MergeDedup /input/merge /output/merge
run "3-2 排序" SortInts /input/sort /output/sort
run "3-3 祖孙关系挖掘" Grandparent /input/family /output/family

echo; echo ">>> 全部完成，作业详情见 http://localhost:8088"
