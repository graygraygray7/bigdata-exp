package sdu.bigdata;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

/**
 * 实验三 第二部分：PageRank 的 Spark 实现（Java API + RDD）。
 *
 * <p>与 MapReduce 版本使用完全相同的公式和迭代方式：
 * {@code PR(p) = (1-d) + d * Σ PR(q)/L(q)}，d = 0.85，迭代 10 轮，
 * 悬挂节点补自环，最终输出 {@code (网页,PR值)}（10 位小数、按 PR 降序）。</p>
 *
 * <p>实现要点：
 * <ul>
 *   <li>links: {@code RDD[(page, List[outlink])]}，用 reduceByKey 合并并去重；</li>
 *   <li>每轮 contributions = links.join(ranks).flatMap(...)，再与 links.keys() 补 0 后
 *       reduceByKey 聚合，保证没有入链的节点也不会从结果中消失；</li>
 *   <li>每轮 ranks.count() 强制物化并 unpersist 上一轮，避免 lineage 无限增长；</li>
 *   <li>最终用 -PR 作为排序键，单分区输出，保证全局按 PR 降序。</li>
 * </ul>
 *
 * <p>提交示例见 scripts/run_pagerank_spark.sh。</p>
 */
public class PageRankSpark {

    public static final double DAMPING = 0.85;
    public static final int DEFAULT_ITERATIONS = 10;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("用法: PageRankSpark <输入邻接表> <输出目录> [迭代次数，默认 10]");
            System.exit(2);
        }
        String input = args[0];
        String output = args[1];
        int iterations = args.length >= 3 ? Integer.parseInt(args[2]) : DEFAULT_ITERATIONS;

        SparkConf conf = new SparkConf().setAppName("PageRank-Spark");
        if (!conf.contains("spark.master")) {
            conf.setMaster("local[*]");
        }
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // 输出目录必须不存在
        try {
            FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
            Path out = new Path(output);
            if (fs.exists(out)) {
                fs.delete(out, true);
            }
        } catch (Exception e) {
            throw new RuntimeException("清理输出目录失败: " + output, e);
        }

        // 1. 邻接表 -> (page, 出链列表)，悬挂节点补自环
        JavaRDD<String> lines = sc.textFile(input);
        JavaPairRDD<String, List<String>> links = lines
                .flatMapToPair(line -> {
                    List<Tuple2<String, List<String>>> result = new ArrayList<>();
                    String s = line.trim();
                    if (s.isEmpty() || s.startsWith("#")) {
                        return result.iterator();
                    }
                    String[] parts = s.split("\t", 2);
                    String src = parts[0].trim();
                    if (src.isEmpty()) {
                        return result.iterator();
                    }
                    List<String> outLinks = new ArrayList<>();
                    if (parts.length > 1) {
                        for (String t : parts[1].split(",")) {
                            t = t.trim();
                            if (!t.isEmpty()) {
                                outLinks.add(t);
                            }
                        }
                    }
                    result.add(new Tuple2<>(src, outLinks));
                    for (String target : outLinks) {
                        result.add(new Tuple2<>(target, new ArrayList<>()));
                    }
                    return result.iterator();
                })
                .reduceByKey((a, b) -> {
                    List<String> merged = new ArrayList<>(a);
                    merged.addAll(b);
                    return merged;
                })
                .mapToPair(kv -> {
                    LinkedHashSet<String> unique = new LinkedHashSet<>(kv._2);
                    List<String> outLinks;
                    if (unique.isEmpty()) {
                        // 悬挂节点补自环
                        outLinks = new ArrayList<>(Arrays.asList(kv._1));
                    } else {
                        outLinks = new ArrayList<>(unique);
                    }
                    return new Tuple2<>(kv._1, outLinks);
                })
                .cache();

        long nodeCount = links.count();
        System.out.println(">>> 节点数: " + nodeCount);

        // 2. 初始 PR = 1.0
        JavaPairRDD<String, Double> ranks = links.keys()
                .mapToPair(p -> new Tuple2<>(p, 1.0))
                .cache();
        ranks.count();

        // 3. 迭代
        for (int round = 1; round <= iterations; round++) {
            JavaPairRDD<String, Double> contributions = links.join(ranks)
                    .flatMapToPair(kv -> {
                        List<String> outLinks = kv._2._1;
                        double pr = kv._2._2;
                        double share = pr / outLinks.size();
                        List<Tuple2<String, Double>> result = new ArrayList<>(outLinks.size());
                        for (String target : outLinks) {
                            result.add(new Tuple2<>(target, share));
                        }
                        return result.iterator();
                    });

            // 先给每个节点补 0，再与贡献合并：没有入链的节点也不会消失
            JavaPairRDD<String, Double> zero = links.keys()
                    .mapToPair(p -> new Tuple2<>(p, 0.0));
            JavaPairRDD<String, Double> newRanks = zero
                    .union(contributions)
                    .reduceByKey((a, b) -> a + b)
                    .mapValues(sum -> (1.0 - DAMPING) + DAMPING * sum)
                    .cache();
            newRanks.count();

            ranks.unpersist();
            ranks = newRanks;
            System.out.println(">>> 第 " + round + "/" + iterations + " 轮迭代完成");
        }

        // 4. 输出 (网页,PR值)，按 PR 降序
        JavaPairRDD<Double, String> sorted = ranks
                .mapToPair(kv -> new Tuple2<>(
                        -kv._2,
                        String.format(Locale.ROOT, "(%s,%.10f)", kv._1, kv._2)))
                .sortByKey(true, 1);
        try {
            sorted.values().saveAsTextFile(output);
        } catch (Exception e) {
            throw new RuntimeException("保存结果失败: " + output, e);
        }

        System.out.println("✅ Spark PageRank 完成，迭代 " + iterations + " 轮，结果: " + output);
        sc.stop();
    }
}
