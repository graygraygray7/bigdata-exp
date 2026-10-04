package sdu.bigdata;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Scanner;

/**
 * 功能(1) 向 HDFS 上传任意文本文件；
 *        若文件已存在，由用户指定：追加到原有文件末尾 或 覆盖原有文件。
 *
 * 对应 Shell：hdfs dfs -put / -appendToFile / -f
 */
public class FileExists {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsApi.getFs();
        Scanner sc = new Scanner(System.in);

        String local, hdfs;
        if (args.length >= 2) {
            local = args[0];
            hdfs = args[1];
        } else {
            System.out.print("本地文件路径: ");
            local = sc.next();
            System.out.print("HDFS 目标路径: ");
            hdfs = sc.next();
        }

        Path dst = new Path(hdfs);
        if (!new java.io.File(local).exists()) {
            System.out.println("本地文件不存在: " + local);
            fs.close();
            return;
        }
        if (fs.exists(dst)) {
            System.out.print("文件已存在，追加到末尾(a) / 覆盖(c)? ");
            String op = args.length >= 3 ? args[2] : sc.next();
            if (op.equalsIgnoreCase("a")) {
                // 追加：打开 HDFS 文件输出流，写入本地文件全部内容
                try (FSDataOutputStream out = fs.append(dst);
                     var in = new BufferedInputStream(new FileInputStream(local))) {
                    byte[] buf = new byte[4096];
                    int n;
                    while ((n = in.read(buf)) > 0) out.write(buf, 0, n);
                }
                System.out.println("已追加到 " + hdfs);
            } else {
                // 覆盖：copyFromLocalFile(src, dst) 遇同名不会覆盖，必须先删再传
                fs.delete(dst, false);
                fs.copyFromLocalFile(new Path(local), dst);
                System.out.println("已覆盖 " + hdfs);
            }
        } else {
            fs.copyFromLocalFile(new Path(local), dst);
            System.out.println("已上传 " + hdfs);
        }

        System.out.println("---- 当前文件内容 ----");
        try (FSDataInputStream in = fs.open(dst)) {
            org.apache.hadoop.io.IOUtils.copyBytes(in, System.out, 4096, false);
        }
        fs.close();
    }
}
