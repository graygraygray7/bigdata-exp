package sdu.bigdata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 实验二：文档倒排索引（Inverted Index）
 *
 * 输入：若干英文文本
 * 输出：每行一个单词，后接其各文档词频及总词频：
 *   word <doc1.txt,6>;<doc2.txt,3>;<total,9>.
 *
 * 流程：
 *   map     : (word, "文件名@1")
 *   shuffle : 框架将同一 word 的 value 聚到同一 reduce
 *   reduce  : 按文件累加词频，TreeMap 保证文件名有序，末尾补 <total,N>
 *
 * 运行：hadoop jar mr-exp-1.0.jar sdu.bigdata.InvertedIndex /input/webdata /output/invindex
 */
public class InvertedIndex {

    /** Mapper：切词、去停词，输出 (单词, "文件名@1")。 */
    public static class M extends Mapper<Object, Text, Text, Text> {

        /** 停词表，setup() 中从分布式缓存加载。 */
        private final Set<String> stopSet = new HashSet<>();

        /** 读取任务工作目录下的 stopwords.txt。 */
        @Override
        protected void setup(Context context) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"))) {
                String w;
                while ((w = br.readLine()) != null) {
                    stopSet.add(w.trim().toLowerCase());
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // 当前分片所属文件名
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

            // 统一小写，按非字母字符切词
            String[] words = value.toString().toLowerCase().split("[^a-z]+");

            for (String w : words) {
                // 过滤空串与停词
                if (w.isEmpty() || stopSet.contains(w)) {
                    continue;
                }
                // key=单词，value=出现位置
                context.write(new Text(w), new Text(filename + "@1"));
            }
        }
    }

    /** Reducer：汇总同一单词在各文档中的词频。 */
    public static class R extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // TreeMap 按文件名字典序排列
            TreeMap<String, Integer> docCnt = new TreeMap<>();
            int total = 0;

            // 拆分 "文件名@1"，按文件累加
            for (Text text : values) {
                String[] kv = text.toString().split("@");
                String filename = kv[0];
                int cnt = Integer.parseInt(kv[1]);
                total += cnt;
                docCnt.merge(filename, cnt, Integer::sum);
            }

            // 拼接 " <f1,c1>;<f2,c2>;...;<total,T>."
            StringBuilder sb = new StringBuilder(" ");
            boolean first = true;
            for (Map.Entry<String, Integer> e : docCnt.entrySet()) {
                if (!first) {
                    sb.append(';');
                }
                sb.append('<').append(e.getKey()).append(',').append(e.getValue()).append('>');
                first = false;
            }
            sb.append(";<total,").append(total).append(">.");

            context.write(key, new Text(sb.toString()));
        }
    }

    /** 作业装配：输入、输出、Mapper/Reducer 与停词表分发。 */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(M.class);
        job.setReducerClass(R.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 停词表分发到每个任务的本地工作目录
        job.addCacheFile(new URI("/input/stopwords.txt#stopwords.txt"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出目录必须不存在
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
