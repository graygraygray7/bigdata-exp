package sdu.bigdata;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 功能(5) 给定 HDFS 中某一个目录，递归输出该目录下
 *        所有文件的读写权限、大小、创建时间、路径等信息。
 *
 * 对应 Shell：hdfs dfs -ls -R <dir> （旧写法 -lsr）
 */
public class LSDir {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsApi.getFs();
        Path dir = new Path(args.length > 0 ? args[0] : HdfsApi.USER_HOME);

        printRecursive(fs, dir);
        fs.close();
    }

    /** 递归打印目录树中每个条目的状态信息 */
    static void printRecursive(FileSystem fs, Path p) throws Exception {
        FileStatus[] list = fs.listStatus(p);
        for (FileStatus s : list) {
            String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    .format(new Date(s.getModificationTime()));
            // %s 权限 %d 副本 %s 属主 %s 属组 %12d 字节 %s 时间 %s 路径
            System.out.printf("%s  %d  %s  %s  %12d  %s  %s%n",
                    s.getPermission(), s.getReplication(), s.getOwner(), s.getGroup(),
                    s.getLen(), time, s.getPath());
            if (s.isDirectory()) {
                printRecursive(fs, s.getPath());
            }
        }
    }
}
