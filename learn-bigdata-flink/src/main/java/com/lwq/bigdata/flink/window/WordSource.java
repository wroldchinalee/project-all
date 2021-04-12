package com.lwq.bigdata.flink.window;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class WordSource implements SourceFunction<String> {
    private boolean isRunning = true;
    private String[] words = {"abc", "def", "hdfs", "flink", "hadoop", "bigdata"};
    private Random random = new Random();

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning) {
            ctx.collect(words[random.nextInt(words.length)]);
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
