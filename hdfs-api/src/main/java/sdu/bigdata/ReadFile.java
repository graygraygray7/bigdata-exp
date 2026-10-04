package sdu.bigdata;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 功能(3) 将 HDFS 中指定文件的内容输出到终端。
 *
 * 对应 Shell：hdfs dfs -cat <path>
 */
public class ReadFile {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsApi.getFs();
        Path p = new Path(args.length > 0 ? args[0] : HdfsApi.USER_HOME + "/local.txt");

        try (FSDataInputStream in = fs.open(p)) {
            org.apache.hadoop.io.IOUtils.copyBytes(in, System.out, 4096, false);
        }
        fs.close();
    }
}
