package sdu.bigdata;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 功能(6) 提供一个 HDFS 内文件的路径，对该文件进行创建和删除操作；
 *        文件所在目录不存在时，自动创建目录。
 *
 * 对应 Shell：hdfs dfs -mkdir -p + -touchz + -rm
 */
public class CreateDeleteFile {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsApi.getFs();

        Path dir = new Path(args.length > 0 ? args[0] : HdfsApi.USER_HOME + "/exp6");
        Path file = new Path(dir, "newfile.txt");

        // 1) 目录不存在则自动创建（自动补全缺失的父目录）
        if (!fs.exists(dir)) {
            fs.mkdirs(dir);
            System.out.println("目录不存在，已自动创建: " + dir);
        }

        // 2) 创建文件（touchz 等价的空文件）
        if (!fs.exists(file)) {
            fs.create(file).close();
            System.out.println("文件已创建: " + file);
        } else {
            System.out.println("文件已存在: " + file);
        }

        // 3) 删除文件
        boolean ok = fs.delete(file, false);
        System.out.println("文件删除" + (ok ? "成功" : "失败"));
        System.out.println("删除后还存在吗: " + fs.exists(file));

        fs.close();
    }
}
