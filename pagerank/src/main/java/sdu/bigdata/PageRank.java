package sdu.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Locale;

/**
 * 实验三 第二部分：PageRank 的 MapReduce 实现。
 *
 * <p>输入每行格式：{@code 网页 \t 出链1,出链2,...}</p>
 *
 * <p>迭代公式：{@code PR(p) = (1-d) + d * Σ PR(q)/L(q)}，d = 0.85。
 * 采用不除以 N 的归一化，全网 PR 之和约等于 N，便于与题目示例（PR 值 1.x）对照。</p>
 *
 * <p>作业拆成三类：
 * <ol>
 *   <li>Init：邻接表 → 节点 \t 初始PR \t 出链。目标节点若从未作为源出现（悬挂节点），补自环，
 *       保证每一轮迭代中所有节点都有出链、PR 总量不流失；</li>
 *   <li>Iterate：一轮迭代。map 同时发出「本节点的出链」和「沿每条出链分走的 PR」，
 *       reduce 按键聚合并计算新 PR，输出仍带出链，供下一轮使用；</li>
 *   <li>Format：读取第 10 轮结果，输出题目要求的 {@code (网页,PR值)}，PR 保留 10 位小数；
 *       用 -PR 作为 key 让结果按 PR 降序排列。</li>
 * </ol>
 */
public class PageRank {

    public static final double DAMPING = 0.85;
    /** 迭代轮数（题目要求 10 次）。 */
    public static final int DEFAULT_ITERATIONS = 10;

    // ==================================================================
    // 1. 初始化：邻接表 -> 节点 \t 1.0 \t 出链（悬挂节点补自环）
    // ==================================================================
    public static class InitMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("#")) {
                return;
            }
            String[] parts = line.split("\t", 2);
            String node = parts[0].trim();
            if (node.isEmpty()) {
                return;
            }
            String links = parts.length > 1 ? parts[1].trim() : "";

            // 本节点的出链（key = 本节点）
            outKey.set(node);
            outValue.set("L:" + links);
            context.write(outKey, outValue);

            // 每个目标也发一条标记，保证「只被链接、从未作为源出现」的节点也进入结果集
            if (!links.isEmpty()) {
                for (String target : links.split(",")) {
                    target = target.trim();
                    if (target.isEmpty() || target.equals(node)) {
                        continue;
                    }
                    outKey.set(target);
                    outValue.set("T");
                    context.write(outKey, outValue);
                }
            }
        }
    }

    public static class InitReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String links = null;
            for (Text value : values) {
                String s = value.toString();
                if (s.startsWith("L:")) {
                    links = s.substring(2).trim();
                }
            }

            LinkedHashSet<String> outLinks = new LinkedHashSet<>();
            if (links != null && !links.isEmpty()) {
                for (String target : links.split(",")) {
                    target = target.trim();
                    if (!target.isEmpty()) {
                        outLinks.add(target);
                    }
                }
            }
            // 悬挂节点（无出链）补自环：PR 留在原地，避免每轮总量衰减
            if (outLinks.isEmpty()) {
                outLinks.add(key.toString());
            }

            outValue.set("1.0\t" + String.join(",", outLinks));
            context.write(key, outValue);
        }
    }

    // ==================================================================
    // 2. 一轮迭代
    // ==================================================================
    public static class IterMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 输入：节点 \t 上一轮PR \t 出链
            String[] parts = value.toString().split("\t", 3);
            if (parts.length < 3) {
                return;
            }
            String node = parts[0];
            String links = parts[2];
            double pr;
            try {
                pr = Double.parseDouble(parts[1]);
            } catch (NumberFormatException e) {
                return;
            }

            // 继续把出链带下去，供下一轮使用
            outKey.set(node);
            outValue.set("L:" + links);
            context.write(outKey, outValue);

            // 把当前 PR 平均分给出链
            if (!links.isEmpty()) {
                String[] targets = links.split(",");
                if (targets.length > 0) {
                    String share = Double.toString(pr / targets.length);
                    for (String target : targets) {
                        if (target.isEmpty()) {
                            continue;
                        }
                        outKey.set(target);
                        outValue.set("P:" + share);
                        context.write(outKey, outValue);
                    }
                }
            }
        }
    }

    public static class IterReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            String links = "";
            for (Text value : values) {
                String s = value.toString();
                if (s.startsWith("L:")) {
                    links = s.substring(2);
                } else if (s.startsWith("P:")) {
                    sum += Double.parseDouble(s.substring(2));
                }
            }
            double pr = (1.0 - DAMPING) + DAMPING * sum;
            outValue.set(Double.toString(pr) + "\t" + links);
            context.write(key, outValue);
        }
    }

    // ==================================================================
    // 3. 输出格式化：按 PR 降序，输出 (网页,PR值)
    // ==================================================================
    public static class FormatMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        private final DoubleWritable outKey = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t", 3);
            if (parts.length < 2) {
                return;
            }
            double pr = Double.parseDouble(parts[1]);
            // 负号：让框架按 -PR 升序排序，等价于按 PR 降序输出
            outKey.set(-pr);
            context.write(outKey, new Text(String.format(Locale.ROOT, "(%s,%.10f)", parts[0], pr)));
        }
    }

    public static class FormatReducer extends Reducer<DoubleWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    // ==================================================================
    // Driver
    // ==================================================================

    private static Configuration jobConf() {
        Configuration conf = new Configuration();
        // 伪分布式内存有限，显式设置容器内存与 JVM 参数，避免默认 512m 不够用
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.map.java.opts", "-Xmx900m -Djava.net.preferIPv4Stack=true");
        conf.set("mapreduce.reduce.memory.mb", "1024");
        conf.set("mapreduce.reduce.java.opts", "-Xmx900m -Djava.net.preferIPv4Stack=true");
        return conf;
    }

    private static Job initJob(Configuration conf, Path input, Path output) throws IOException {
        Job job = Job.getInstance(conf, "PageRank-Init");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(InitMapper.class);
        job.setReducerClass(InitReducer.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    private static Job iterationJob(Configuration conf, Path input, Path output, int round)
            throws IOException {
        Job job = Job.getInstance(conf, "PageRank-Iter-" + round);
        job.setJarByClass(PageRank.class);
        job.setMapperClass(IterMapper.class);
        job.setReducerClass(IterReducer.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    private static Job formatJob(Configuration conf, Path input, Path output) throws IOException {
        Job job = Job.getInstance(conf, "PageRank-Format");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(FormatMapper.class);
        job.setReducerClass(FormatReducer.class);
        // 1 个 reduce：全局按 PR 降序输出
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("用法: PageRank <输入邻接表(HDFS)> <输出目录(HDFS)> [迭代次数，默认 10]");
            System.exit(2);
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        int iterations = args.length >= 3 ? Integer.parseInt(args[2]) : DEFAULT_ITERATIONS;

        Configuration conf = jobConf();
        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        long start = System.currentTimeMillis();

        // 初始化：iter0
        Path current = new Path(output, "iter0");
        System.out.println(">>> [1/2] 初始化 PageRank（补自环、PR=1.0）");
        if (!initJob(conf, input, current).waitForCompletion(true)) {
            System.err.println("初始化作业失败");
            System.exit(1);
        }

        // 10 轮迭代
        for (int round = 1; round <= iterations; round++) {
            Path next = new Path(output, "iter" + round);
            System.out.println(">>> [2/2] 第 " + round + "/" + iterations + " 轮迭代");
            if (!iterationJob(conf, current, next, round).waitForCompletion(true)) {
                System.err.println("第 " + round + " 轮迭代失败");
                System.exit(1);
            }
            current = next;
        }

        // 格式化输出
        Path result = new Path(output, "result");
        System.out.println(">>> 格式化输出（(网页,PR值)，PR 保留 10 位小数，按 PR 降序）");
        if (!formatJob(conf, current, result).waitForCompletion(true)) {
            System.err.println("格式化作业失败");
            System.exit(1);
        }

        // 删除中间迭代目录，只保留 result
        for (int round = 0; round <= iterations; round++) {
            fs.delete(new Path(output, "iter" + round), true);
        }

        long seconds = (System.currentTimeMillis() - start) / 1000;
        System.out.println("✅ PageRank 完成，迭代 " + iterations + " 轮，耗时 " + seconds + " 秒");
        System.out.println("   结果目录: " + result);
    }
}
