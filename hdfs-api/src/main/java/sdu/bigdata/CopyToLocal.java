package sdu.bigdata;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;

/**
 * 功能(2) 从 HDFS 下载指定文件；
 *        本地已有同名文件时，自动重命名（加 .1、.2 ... 后缀）再下载。
 *
 * 对应 Shell：hdfs dfs -get（带同名检测与重命名逻辑）
 */
public class CopyToLocal {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsApi.getFs();

        Path src = new Path(args.length > 0 ? args[0] : HdfsApi.USER_HOME + "/local.txt");
        String local = args.length > 1 ? args[1] : "/tmp/hdfs_exp/local.txt";

        // 同名自动重命名：local.txt -> local.txt.1 -> local.txt.2 ...
        File f = new File(local);
        while (f.exists()) {
            local = local + ".1";
            f = new File(local);
        }

        fs.copyToLocalFile(src, new Path(local));
        System.out.println("已下载到 " + local);

        // 顺带演示功能(3)：把 HDFS 文件内容直接输出到终端
        System.out.println("---- " + src + " 内容 ----");
        try (FSDataInputStream in = fs.open(src)) {
            org.apache.hadoop.io.IOUtils.copyBytes(in, System.out, 4096, false);
        }
        fs.close();
    }
}
