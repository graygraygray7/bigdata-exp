# HBase 本机配置备份

- `hbase-site.xml`：完整复制自 `/home/gray/hbase-2.5.16/conf/hbase-site.xml`（伪分布式 + FSWAL）。
- `hbase-env.append.sh`：完整复制自 `conf/hbase-env.sh`，本机追加的关键内容在文件末尾
  “本机伪分布式配置”注释之后。

部署到新机器时，把这两个文件放到 HBase 的 `conf/` 下，并按实际安装路径修改
`JAVA_HOME` / `HADOOP_HOME` / `hbase.rootdir` 等路径即可。
