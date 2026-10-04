#!/usr/bin/env bash
# 一键编译：javac + hadoop classpath（无需 Maven）  用法: bash doc/build.sh  产物: mr-exp-1.0.jar
set -euo pipefail
cd "$(dirname "$0")/.."

export PATH="/home/gray/java11/bin:/home/gray/hadoop-3.3.6/bin:$PATH"
mkdir -p classes
find src/main/java -name '*.java' | xargs javac -encoding UTF-8 -cp "$(/home/gray/hadoop-3.3.6/bin/hadoop classpath)" -d classes
jar cf mr-exp-1.0.jar -C classes .
echo "✔ 编译完成: $(pwd)/mr-exp-1.0.jar"
