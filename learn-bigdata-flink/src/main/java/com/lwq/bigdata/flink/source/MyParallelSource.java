package com.lwq.bigdata.flink.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * Created by Administrator on 2020-12-06.
 */
public class MyParallelSource implements ParallelSourceFunction<Long> {
    private long number;
    private boolean isRunning = true;


    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            number++;
            ctx.collect(number);
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
