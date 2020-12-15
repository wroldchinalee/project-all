package com.lwq.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author: LWQ
 * @create: 2020/11/22
 * @description: WordCountLocal
 **/
public class WordCountLocal {
    public static void main(String[] args) {
        // 配置
        SparkConf sparkConf = new SparkConf()
                .setAppName("word count local demo")
                .setMaster("local");

        // 创建context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        // 读取输入文件
        JavaRDD<String> inputRDD = sparkContext.textFile("learn-bigdata-spark/doc/words.txt");
        // 下面开始transform
        // 第一步每行的数据split，并拉平
        JavaRDD<String> flatRDD = inputRDD.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());
        // 第二步转换为key-value形式
        JavaPairRDD<String, Long> pairRDD = flatRDD.mapToPair((PairFunction<String, String, Long>) s -> new Tuple2<>(s, 1L));

        // 第三步进行聚合，将同样的work进行累加
        JavaPairRDD<String, Long> resultRDD = pairRDD.reduceByKey((Function2<Long, Long, Long>) (v1, v2) -> v1 + v2);

        List<Tuple2<String, Long>> collect = resultRDD.collect();
        collect.forEach(tuple -> {
            System.out.printf("%s:%d\n", tuple._1, tuple._2);
        });
        sparkContext.close();
    }
}
