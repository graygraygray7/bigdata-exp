package sdu.bigdata;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 功能(4) 显示 HDFS 指定文件的读写权限、大小、创建时间、路径等信息。
 *
 * 对应 Shell：hdfs dfs -ls <file>
 */
public class LSFile {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsApi.getFs();
        Path p = new Path(args.length > 0 ? args[0] : HdfsApi.USER_HOME + "/local.txt");

        FileStatus s = fs.getFileStatus(p);
        System.out.println("路径     : " + s.getPath());
        System.out.println("权限     : " + s.getPermission());
        System.out.println("副本数   : " + s.getReplication());
        System.out.println("属主     : " + s.getOwner());
        System.out.println("属组     : " + s.getGroup());
        System.out.println("大小     : " + s.getLen() + " 字节");
        System.out.println("修改时间 : " +
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(s.getModificationTime())));

        fs.close();
    }
}
