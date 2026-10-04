package sdu.bigdata;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 功能(9) 删除 HDFS 中指定的文件。
 *
 * 对应 Shell：hdfs dfs -rm <path>
 */
public class DeleteFile {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsApi.getFs();
        Path p = new Path(args.length > 0 ? args[0] : HdfsApi.USER_HOME + "/moved.txt");

        if (!fs.exists(p)) {
            System.out.println("文件不存在: " + p);
        } else {
            boolean ok = fs.delete(p, false);
            System.out.println("文件删除" + (ok ? "成功" : "失败") + ": " + p);
        }
        fs.close();
    }
}
