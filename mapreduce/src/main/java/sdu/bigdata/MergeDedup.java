package sdu.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 实验3-1 合并去重：把输入目录下多个文件（A、B）的所有行合并到一起，
 * 剔除重复行，并按键排序，得到输出文件 C。
 *
 * 核心思路：map 把「整行内容」当作 key 输出。框架会自动把相同 key 聚到同一个
 * reduce 且按键排序，所以 reduce 里每个 key 只需要输出一次 —— 去重和排序
 * 都是框架"免费"送的。
 *
 * 运行：hadoop jar mr-exp-1.0.jar sdu.bigdata.MergeDedup /input/merge /output/merge
 */
public class MergeDedup {

    /**
     * 泛型四个类型依次是：<输入key, 输入value, 输出key, 输出value>
     * 输入 key   = 行首在文件里的字节偏移量（我们用不到，写 Object 即可）
     * 输入 value = 一行文本（Text 是 Hadoop 版的 String）
     * 输出 key   = 整行内容
     * 输出 value = NullWritable（Hadoop 里的"没有值"占位符）
     */
    public static class M extends Mapper<Object, Text, Text, NullWritable> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();     // Text -> String
            if (!line.isEmpty()) {
                context.write(new Text(line), NullWritable.get());  // 发出 (整行, 无值)
            }
        }
    }

    /**
     * reduce 收到的：同一个 key（同一行内容）出现过的所有 value。
     * 同一行内容不管在 A、B 里出现几次，都只会进这一个 reduce —— 天然分组。
     */
    public static class R extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());    // 只写一次 → 去重
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();          // 读取 core-site.xml 等配置
        Job job = Job.getInstance(conf, "merge dedup");    // 起一个作业，名字会显示在 YARN UI
        job.setJarByClass(MergeDedup.class);               // 告诉框架代码在哪个 jar 里
        job.setMapperClass(M.class);                       // 挂上我们的 Mapper
        job.setReducerClass(R.class);                      // 挂上我们的 Reducer
        job.setMapOutputKeyClass(Text.class);              // map 输出 key 的类型
        job.setMapOutputValueClass(NullWritable.class);    // map 输出 value 的类型
        job.setOutputKeyClass(Text.class);                 // 最终输出 key 类型
        job.setOutputValueClass(NullWritable.class);       // 最终输出 value 类型
        FileInputFormat.addInputPath(job, new Path(args[0]));    // 输入目录（HDFS）
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // 输出目录（必须不存在）
        System.exit(job.waitForCompletion(true) ? 0 : 1);        // 提交并等结束
    }
}
