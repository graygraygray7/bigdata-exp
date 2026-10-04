#!/usr/bin/env bash
# HDFS Java API 实验一键演示：编译 → 造数据 → 依次运行 11 个 API → 验证
# 用法: bash doc/run_all.sh   （可重复运行）
set -euo pipefail
cd "$(dirname "$0")/.."

export PATH="/home/gray/java11/bin:/home/gray/hadoop-3.3.6/bin:$PATH"
export HADOOP_CLASSPATH=$PWD/hdfs-api-1.0.jar
J=sdu.bigdata

[ -f hdfs-api-1.0.jar ] || bash doc/build.sh

run(){ echo; echo "=== $1 ==="; shift; "$@"; }

mkdir -p /tmp/hdfs_exp
echo "hello hdfs          " > /tmp/hdfs_exp/local.txt
echo "line-for-append     " > /tmp/hdfs_exp/append.txt

run "1. FileExists: 上传（c=覆盖 a=追加）" hadoop $J.FileExists /tmp/hdfs_exp/local.txt /user/gray/local.txt c
hdfs dfs -cat /user/gray/local.txt

echo "placeholder 同名旧文件" > /tmp/hdfs_exp/dl_test.txt     # 预置同名文件，触发下载重命名
run "2. CopyToLocal: 下载（同名自动 .1 重命名）" hadoop $J.CopyToLocal /user/gray/local.txt /tmp/hdfs_exp/dl_test.txt
run "3. ReadFile: 输出文件内容" hadoop $J.ReadFile /user/gray/local.txt
run "4. LSFile: 单文件信息" hadoop $J.LSFile /user/gray/local.txt
run "5. LSDir: 递归目录信息" hadoop $J.LSDir /user/gray
run "6. CreateDeleteFile: 建/删文件（自动建目录）" hadoop $J.CreateDeleteFile /user/gray/exp6
run "7. CreateDeleteDir: 建/删目录（空才删）" hadoop $J.CreateDeleteDir /user/gray/exp7
run "8. AppendFile: 追加到结尾/开头" hadoop $J.AppendFile /tmp/hdfs_exp/append.txt /user/gray/local.txt e
run "10. MoveFile: HDFS 内移动" hadoop $J.MoveFile /user/gray/local.txt /user/gray/moved.txt
run "9. DeleteFile: 删除文件" hadoop $J.DeleteFile /user/gray/moved.txt

echo; echo "=== 全部功能演示完毕 ==="
