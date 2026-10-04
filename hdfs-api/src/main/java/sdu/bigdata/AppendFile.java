package sdu.bigdata;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.nio.file.Files;
import java.util.Scanner;

/**
 * 功能(8) 向 HDFS 中指定的文件追加内容，
 *        由用户指定内容追加到原有文件的开头 或 结尾。
 *
 * 对应 Shell：hdfs dfs -appendToFile（结尾）；开头无直接命令，需先合并再覆盖
 */
public class AppendFile {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsApi.getFs();
        Scanner sc = new Scanner(System.in);

        String local, hdfs, op;
        if (args.length >= 3) {
            local = args[0];
            hdfs = args[1];
            op = args[2];   // b 或 e
        } else {
            System.out.print("本地追加内容文件: ");
            local = sc.next();
            System.out.print("HDFS 目标文件: ");
            hdfs = sc.next();
            System.out.print("追加到开头(b) / 结尾(e)? ");
            op = sc.next();
        }

        Path dst = new Path(hdfs);
        if (!fs.exists(dst)) {
            System.out.println("HDFS 目标文件不存在: " + hdfs);
            fs.close();
            return;
        }
        byte[] data = Files.readAllBytes(new File(local).toPath());

        if (op.equalsIgnoreCase("e")) {
            // ---- 追加到结尾：直接 append ----
            try (FSDataOutputStream out = fs.append(dst)) {
                out.write(data);
            }
            System.out.println("已追加到结尾");
        } else {
            // ---- 追加到开头：读原文件到临时文件，再整体重写（新内容在前）----
            File tmp = File.createTempFile("hdfs-head", ".tmp");
            tmp.deleteOnExit();
            try (FSDataInputStream in = fs.open(dst)) {
                // createTempFile 已创建文件，必须 REPLACE_EXISTING，否则抛 FileAlreadyExistsException
                Files.copy(in, tmp.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
            try (FSDataOutputStream out = fs.create(dst, true)) {  // overwrite=true
                out.write(data);                        // 先写追加内容
                out.write(Files.readAllBytes(tmp.toPath())); // 再写原内容
            }
            System.out.println("已追加到开头");
        }

        // 打印追加后的内容以便验证
        System.out.println("---- 追加后文件内容 ----");
        try (FSDataInputStream in = fs.open(dst)) {
            org.apache.hadoop.io.IOUtils.copyBytes(in, System.out, 4096, false);
        }
        fs.close();
    }
}
