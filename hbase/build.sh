#!/usr/bin/env bash
# 一键编译 HBase Java API 示例：javac + hbase classpath（无需 Maven）
# 用法: bash hbase/build.sh    产物: hbase/hbase-demo-1.0.jar
set -euo pipefail
cd "$(dirname "$0")"

export JAVA_HOME=/home/gray/java11
export HADOOP_HOME=/home/gray/hadoop-3.3.6
export HBASE_HOME=/home/gray/hbase-2.5.16
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HBASE_HOME/bin:$PATH"

CP="$("$HBASE_HOME/bin/hbase" classpath 2>/dev/null)"
mkdir -p classes
find src/main/java -name '*.java' | xargs "$JAVA_HOME/bin/javac" -encoding UTF-8 -cp "$CP" -d classes
"$JAVA_HOME/bin/jar" cf hbase-demo-1.0.jar -C classes .
echo "✔ 编译完成: $(pwd)/hbase-demo-1.0.jar"
