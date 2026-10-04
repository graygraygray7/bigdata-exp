package sdu.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Grandparent {

    public static class M extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // split("\\s+")：按"一个或多个空白字符"（空格/Tab）切开，\\s 是正则
            String[] t = value.toString().trim().split("\\s+");
            if (t.length != 2 || t[0].equals("child")) {
                return;                       // 跳过空行和表头行 "child  parent"
            }
            context.write(new Text(t[1]), new Text("c:" + t[0]));  // 我当了 parent
            context.write(new Text(t[0]), new Text("p:" + t[1]));  // 我是 child
        }
    }

    public static class R extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text person, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> myChildren = new ArrayList<>();    // 这个人的孩子
            List<String> myParents  = new ArrayList<>();    // 这个人的父母
            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("c:")) {
                    myChildren.add(s.substring(2));         // 去掉前缀 "c:"
                } else {
                    myParents.add(s.substring(2));          // 去掉前缀 "p:"
                }
            }
            // 笛卡尔积：每个孩子 × 每个父母 = 一对祖孙
            for (String grandchild : myChildren) {
                for (String grandparent : myParents) {
                    context.write(new Text(grandchild), new Text(grandparent));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "grandparent");
        job.setJarByClass(Grandparent.class);
        job.setMapperClass(M.class);
        job.setReducerClass(R.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
