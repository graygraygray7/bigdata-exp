#!/usr/bin/env python3
"""对比 MapReduce 与 Spark 两份 PageRank 结果的一致性。

用法:
  python3 compare_results.py output/pagerank_mr_result.txt output/pagerank_spark_result.txt
"""
import re
import sys
from collections import defaultdict

PATTERN = re.compile(r"^\((.+),([0-9.]+)\)$")


def load(path):
    result = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            m = PATTERN.match(line)
            if not m:
                print("无法解析的行: %s" % line, file=sys.stderr)
                continue
            result[m.group(1)] = float(m.group(2))
    return result


def main():
    if len(sys.argv) != 3:
        print("用法: compare_results.py <mr_result> <spark_result>", file=sys.stderr)
        sys.exit(2)
    mr = load(sys.argv[1])
    spark = load(sys.argv[2])

    print("MapReduce 节点数: %d" % len(mr))
    print("Spark     节点数: %d" % len(spark))
    only_mr = set(mr) - set(spark)
    only_spark = set(spark) - set(mr)
    print("仅 MR 有: %d，仅 Spark 有: %d" % (len(only_mr), len(only_spark)))

    common = sorted(set(mr) & set(spark))
    max_diff = 0.0
    max_node = None
    equal_10dp = 0
    for node in common:
        diff = abs(mr[node] - spark[node])
        if diff > max_diff:
            max_diff, max_node = diff, node
        if abs(mr[node] - spark[node]) < 5e-11:
            equal_10dp += 1

    print("公共节点数: %d" % len(common))
    print("10 位小数完全相同: %d (%.2f%%)" % (equal_10dp, 100.0 * equal_10dp / max(1, len(common))))
    print("最大绝对误差: %.3e (节点 %s)" % (max_diff, max_node))
    print("MR    PR 总和: %.6f" % sum(mr.values()))
    print("Spark PR 总和: %.6f" % sum(spark.values()))

    top = sorted(common, key=lambda n: mr[n], reverse=True)[:10]
    print("\nTop10 对照 (MR / Spark):")
    for node in top:
        print("  %-10s %.10f  %.10f" % (node, mr[node], spark[node]))

    ok = not only_mr and not only_spark and max_diff < 1e-9
    print("\n%s" % ("✅ 两份结果一致（误差 < 1e-9）" if ok else "❌ 结果不一致"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
