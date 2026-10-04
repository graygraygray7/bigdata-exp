package sdu.bigdata;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 功能(10) 在 HDFS 中，将文件从源路径移动到目的路径。
 *
 * 对应 Shell：hdfs dfs -mv <src> <dst>
 */
public class MoveFile {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsApi.getFs();

        Path src = new Path(args.length > 1 ? args[0] : HdfsApi.USER_HOME + "/local.txt");
        Path dst = new Path(args.length > 1 ? args[1] : HdfsApi.USER_HOME + "/moved.txt");

        if (!fs.exists(src)) {
            System.out.println("源文件不存在: " + src);
        } else {
            boolean ok = fs.rename(src, dst);
            System.out.println((ok ? "移动成功" : "移动失败") + ": " + src + " -> " + dst);
        }
        fs.close();
    }
}
