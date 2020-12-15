package com.lwq.bigdata.flink.tablesql;

import com.lwq.bigdata.flink.Sensor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.util.Random;

/**
 * Created by Administrator on 2020-12-14.
 */
public class RowSource implements SourceFunction<Row> {
    private boolean running = true;
    Random random = new Random();

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        while (running) {
            String id = "sensor_" + (random.nextInt(5) + 1);
            Long timestamp = System.currentTimeMillis() + random.nextInt(5000);
            Double temp = random.nextDouble() * 30 + 5;
            Row row = new Row(3);
            row.setField(0, id);
            row.setField(1, timestamp);
            row.setField(2, temp);
            System.out.printf("发送数据:%s\n", row);
            ctx.collect(row);
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
