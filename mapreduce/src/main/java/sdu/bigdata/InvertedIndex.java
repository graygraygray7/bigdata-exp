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
 * 实验二 文档倒排索引（Inverted Index）
 *
 * 目标输出（每行一个单词）：
 *   word <document01.txt,6>;<document02.txt,3>;<total,9>.
 *   （文件名按字典序排好；<total,N> 即该词在全部文档里的总次数）
 *
 * 整体思路（复用实验一套路：设计好 (key, value) 就赢了一半）：
 *   map     : 每个单词记为 (word, "文件名@1")                —— 谁出现？在哪个文件？
 *   shuffle : 框架自动把同一个 word 的所有 "文件名@1" 聚到同一个 reduce（白送的）
 *   reduce  : 把 "文件名@1" 逐个累加成每文件出现次数 -> 按文件名排序拼串 -> 追加 <total,N>.
 *
 * 停词表：main() 里用 addCacheFile 把 /input/stopwords.txt 分发给每个 map 任务，
 *         setup() 按工作目录里的 stopwords.txt 读入 HashSet，map 时直接查。
 *
 * 🚩 文件里共有 7 个填空 TODO①~⑦，全部填完才能编译运行（逐个消除报错），
 *    全部填空都在一行或一小段内完成，提示就写在填空的上一行。
 *
 * 运行：hadoop jar mr-exp-1.0.jar sdu.bigdata.InvertedIndex /input/webdata /output/invindex
 * 一键：./scripts/run_inverted_index.sh（先填空 + 再 bash mapreduce/doc/build.sh）
 */
public class InvertedIndex {

    // ============================================================
    // Mapper：一行 -> 若干 (单词, "文件名@1")
    // 四个泛型：<输入key, 输入value, 输出key, 输出value>
    // ============================================================
    public static class M extends Mapper<Object, Text, Text, Text> {

        /** 停词表：setup() 里读入，map() 里查询 */
        private final Set<String> stopSet = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            // 停词表文件在作业启动时被复制到本任务的工作目录（见 main 里的 TODO⑤）
            BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"));
            String w;
            while ((w = br.readLine()) != null) {
                stopSet.add(w.trim().toLowerCase());   // 统一小写比对
            }
            br.close();
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // 当前分片来自哪个输入文件（小文件 = 一个 map 任务处理一个完整文件）
            FileSplit split = (FileSplit) context.getInputSplit();

            // ---------- TODO①：取出分片所属文件的文件名，如 "gnu-make.txt"（一行） ----------
            // 提示：split.getPath() 拿到输入路径 Path，链式再调用取"文件名"的方法
            String filename = ______________________________;

            // 统一转小写再切词：这样 Apple / apple 算同一个词
            String line = value.toString().toLowerCase();

            // ---------- TODO②：按非英文字母把整行切成单词数组（一行） ----------
            // 提示：String 的 split(正则)，已经纯小写了，分隔符可以用正则 [^a-z]+
            String[] words = ______________________________;

            for (String w : words) {

                // ---------- TODO③：跳过空串和停词表里的词（一行 if + continue） ----------
                // 提示：转小写切分后首尾可能出现空串；停词用 stopSet.contains(w) 判断
                ______________________________

                // ---------- TODO④：发射 (单词, "文件名@1")（一行） ----------
                // 提示：context.write(第一个参数 new Text(w)，第二个参数把 文件名 与 "@1" 字符串拼接)
                ______________________________
            }   // for
        }   // map
    }   // M

    // ============================================================
    // Reducer：同一个单词 -> 各文档出现次数 -> 按文件名排序输出
    // ============================================================
    public static class R extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            /** 文件名 -> 该单词在此文件的出现次数（TreeMap 自动按文件名字典序） */
            TreeMap<String, Integer> docCnt = new TreeMap<>();
            /** 该单词在所有文档里的总次数 */
            int total = 0;

            for (Text text : values) {
                String[] kv = text.toString().split("@");   // 形如 "gnu-make.txt@1"
                String filename = kv[0];
                int cnt = Integer.parseInt(kv[1]);
                total += cnt;   // 顺手累计总次数

                // ---------- TODO⑥：把 cnt 累加到 docCnt 中 filename 名下（一小段即可） ----------
                // 提示：已有则 get+put 加上去，没有则 put 进 0 再加；或一行 docCnt.merge(k, cnt, (a,b)->a+b)
                ______________________________
            }

            // ---------- TODO⑦：拼输出串 " <f1,c1>;<f2,c2>;...;<total,T>." ----------
            // 注意 3 个细节：
            //   1) 单词与首个 '<' 之间是 1 个空格（sb 已经预放了空格）
            //   2) 各条目之间用 ';' 分隔 —— 第一个条目拼 "<f,c>"，后面的要先拼 ';'
            //   3) 循环结束后追加 ";<total,总次数>."（连最后的句点）
            // 提示：for (Map.Entry<String, Integer> e : docCnt.entrySet())  —— TreeMap 已按文件名升序
            StringBuilder sb = new StringBuilder(" ");
            ______________________________

            context.write(key, new Text(sb.toString()));
        }   // reduce
    }   // R

    // ============================================================
    // main：作业装配（除了 TODO⑤ 都是样板代码，实验一里写过一遍）
    // ============================================================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(M.class);
        job.setReducerClass(R.class);
        job.setMapOutputKeyClass(Text.class);       // map 输出 key —— 单词
        job.setMapOutputValueClass(Text.class);     // map 输出 value —— "文件名@1"
        job.setOutputKeyClass(Text.class);          // 最终输出 key —— 单词
        job.setOutputValueClass(Text.class);        // 最终输出 value —— 拼好的结果串

        // ---------- TODO⑤：把 HDFS 上的停词表分发到每个任务的本地工作目录（一行） ----------
        // 格式：new URI("/HDFS上的停词表路径#工作目录里的文件名")，
        //       路径按本机清单是 /input/stopwords.txt；# 后面的名字 setup() 里按文件名直接读
        job.addCacheFile(______________________________);

        FileInputFormat.addInputPath(job, new Path(args[0]));    // 输入目录，如 /input/webdata
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // 输出目录，如 /output/invindex（必须不存在）
        System.exit(job.waitForCompletion(true) ? 0 : 1);        // 提交并等待结束
    }
}
