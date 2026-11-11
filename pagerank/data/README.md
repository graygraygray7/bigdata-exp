# PageRank 数据集

输入格式：每行一个网页，`网页 \t 出链1,出链2,...`。

## sample-graph.txt（已提交，50 KB）

从 Stanford Web Graph 中取首次出现的前 1000 个节点构成的诱导子图，用于快速验证：

```bash
python3 pagerank/scripts/prepare_web_graph.py web-Stanford.txt.gz \
  pagerank/data/sample-graph.txt --max-nodes 1000
```

- 节点 1000、边 6865、悬挂节点 141。

## web-graph.txt（不提交，17 MB）

全量 Stanford Web Graph（281,903 节点、2,312,497 条边）。生成方式：

```bash
curl -LO https://snap.stanford.edu/data/web-Stanford.txt.gz
python3 pagerank/scripts/prepare_web_graph.py web-Stanford.txt.gz pagerank/data/web-graph.txt
```

原始数据来源：https://snap.stanford.edu/data/web-Stanford.html
