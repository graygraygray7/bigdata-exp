#!/usr/bin/env bash
# 一键编译 PageRank MapReduce：javac + hadoop classpath（无需 Maven）
# 用法: bash pagerank/build.sh    产物: pagerank/pagerank-1.0.jar
set -euo pipefail
cd "$(dirname "$0")"

export JAVA_HOME=/home/gray/java11
export HADOOP_HOME=/home/gray/hadoop-3.3.6
export SPARK_HOME=/home/gray/spark-3.5.9-bin-hadoop3
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"

CP="$("$HADOOP_HOME/bin/hadoop" classpath):$SPARK_HOME/jars/*"
mkdir -p classes
find src/main/java -name '*.java' | xargs "$JAVA_HOME/bin/javac" -encoding UTF-8 -cp "$CP" -d classes
"$JAVA_HOME/bin/jar" cf pagerank-1.0.jar -C classes .
echo "✔ 编译完成: $(pwd)/pagerank-1.0.jar"
