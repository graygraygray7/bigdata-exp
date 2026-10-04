package sdu.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * 公共工具类：提供全局唯一的 FileSystem。
 * 注意：本机 fs.defaultFS 为 hdfs://localhost:19001
 * （WSL virtioproxy 对回环端口转发不稳定，端口沿革 9000 -> 19000 -> 19001），
 * Java API 必须显式 set，不能依赖默认 9000。
 */
public class HdfsApi {

    public static final String HDFS_URI = "hdfs://localhost:19001";
    public static final String USER_HOME = "/user/gray";

    private static FileSystem fs;

    private HdfsApi() {}

    /** 获取全局共享的 FileSystem（每次实验类单独运行，无需关心并发） */
    public static synchronized FileSystem getFs() throws Exception {
        if (fs == null) {
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            conf.set("fs.defaultFS", HDFS_URI);
            fs = FileSystem.get(conf);
        }
        return fs;
    }
}
