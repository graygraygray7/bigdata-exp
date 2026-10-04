package sdu.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class SortInts {

    /** 输出 key = 数字本身（框架按它升序），输出 value = 1（占位，实际用不到） */
    public static class M extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String s = value.toString().trim();
            if (s.isEmpty()) {
                return;                                    // 跳过空行
            }
            int num = Integer.parseInt(s);                 // 字符串 -> int
            context.write(new IntWritable(num), new IntWritable(1));
        }
    }

    public static class R extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private int rank = 0;

        @Override
        public void setup(Context context) {               // 每个 reduce 任务开始时调用一次
            rank = 0;
        }

        @Override
        public void reduce(IntWritable num, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            rank++;                                        // 相同数字并列占一个名次（样例无重复数）
            context.write(new IntWritable(rank), num);     // 输出 (名次, 数字)
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort ints");
        job.setJarByClass(SortInts.class);
        job.setMapperClass(M.class);
        job.setReducerClass(R.class);
        job.setNumReduceTasks(1);      // 必须 1 个 reduce，否则每个 reduce 各自从 1 数起、名次会重复
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
