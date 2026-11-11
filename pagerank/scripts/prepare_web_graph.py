#!/usr/bin/env python3
"""把 SNAP 的边列表转换成 PageRank 输入格式：`网页\\t出链1,出链2,...`。

数据来源：Stanford Web Graph (2002)
  https://snap.stanford.edu/data/web-Stanford.html
  281,903 个网页、2,312,497 条有向边。

用法:
  python3 prepare_web_graph.py web-Stanford.txt.gz data/web-graph.txt
  python3 prepare_web_graph.py web-Stanford.txt.gz data/sample-graph.txt --max-nodes 1000

--max-nodes N 只保留首次出现的前 N 个节点及其之间的边，用于生成仓库内的快速测试样本。
"""
import argparse
import gzip
import sys
from collections import OrderedDict


def open_maybe_gzip(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="SNAP 边列表（每行 src\\tdst，可 .gz）")
    parser.add_argument("output", help="输出邻接表")
    parser.add_argument("--max-nodes", type=int, default=0,
                        help="只保留前 N 个节点构成的诱导子图（0 表示全图）")
    args = parser.parse_args()

    allowed = set()
    adj = OrderedDict()
    edge_count = 0
    with open_maybe_gzip(args.input) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            src, dst = parts[0], parts[1]
            if args.max_nodes:
                if src not in allowed:
                    if len(allowed) >= args.max_nodes:
                        continue
                    allowed.add(src)
                if dst not in allowed:
                    if len(allowed) >= args.max_nodes:
                        continue
                    allowed.add(dst)
            adj.setdefault(src, []).append(dst)
            edge_count += 1

    # 目标节点即使没有出链也保留在节点集合中（后续 MapReduce/Spark 会给悬挂节点补自环）
    nodes = set(adj.keys())
    for targets in adj.values():
        nodes.update(targets)

    with open(args.output, "w", encoding="utf-8") as out:
        for src, targets in adj.items():
            # 去重并保持出现顺序
            seen = set()
            unique = []
            for t in targets:
                if t not in seen:
                    seen.add(t)
                    unique.append(t)
            out.write(src + "\t" + ",".join(unique) + "\n")

    sinks = sum(1 for n in nodes if not adj.get(n))
    print(f"输入边数      : {edge_count}")
    print(f"节点数        : {len(nodes)}")
    print(f"有出链的节点  : {len(adj)}")
    print(f"悬挂节点(无出链): {sinks}")
    print(f"输出文件      : {args.output}")


if __name__ == "__main__":
    sys.exit(main())
