package sdu.bigdata;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 功能(7) 提供一个 HDFS 目录的路径，对该目录进行创建和删除操作。
 *        - 创建：若父目录不存在，自动创建（等价 -mkdir -p）
 *        - 删除：目录为空时才删除；不为空时不删除（等价 -rmdir，不用 -rm -r）
 *
 * 对应 Shell：hdfs dfs -mkdir -p / -rmdir
 */
public class CreateDeleteDir {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsApi.getFs();

        Path dir = new Path(args.length > 0 ? args[0] : HdfsApi.USER_HOME + "/a/b/exp7");

        // ---- 创建：父目录不存在时自动创建 ----
        if (!fs.exists(dir)) {
            fs.mkdirs(dir);
            System.out.println("目录不存在，已自动创建（含父目录）: " + dir);
        } else {
            System.out.println("目录已存在: " + dir);
        }

        // ---- 删除：仅当为空时删除，非空不删 ----
        FileStatus[] children = fs.listStatus(dir);
        if (children.length == 0) {
            boolean ok = fs.delete(dir, false);   // 第二个参数 false：不递归
            System.out.println("目录为空 → 删除" + (ok ? "成功" : "失败"));
        } else {
            System.out.println("目录非空（含 " + children.length + " 个条目）→ 不删除");
        }

        fs.close();
    }
}
