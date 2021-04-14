package com.lwq.bigdata.flink.mix;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * Created by Administrator on 2020-12-19.
 */
public class MySource1 implements SourceFunction<String> {
    private boolean running = true;
    private String[] words = {"hello", "world", "spark", "flink", "hadoop", "abc", "nx", "def", "hbase"};
    private Random random = new Random();

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (running) {
            int count = random.nextInt(4) + 1;
            String wordConcat = "";
            for (int i = 0; i < count; i++) {
                String word = words[random.nextInt(words.length)];
                if (wordConcat.isEmpty()) {
                    wordConcat = word;
                } else {
                    wordConcat = wordConcat + "," + word;
                }
            }
            ctx.collect(wordConcat);
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
