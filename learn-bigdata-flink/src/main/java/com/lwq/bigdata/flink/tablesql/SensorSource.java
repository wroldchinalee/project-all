package com.lwq.bigdata.flink.tablesql;

import com.lwq.bigdata.flink.Sensor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.util.Random;

/**
 * Created by Administrator on 2020-12-14.
 */
public class SensorSource implements SourceFunction<Sensor> {
    private boolean running = true;
    Random random = new Random();

    @Override
    public void run(SourceContext<Sensor> ctx) throws Exception {
        while (running) {
            String id = "sensor_" + (random.nextInt(5) + 1);
            Long timestamp = System.currentTimeMillis() + random.nextInt(5000);
            Double temp = random.nextDouble() * 30 + 5;
            Sensor sensor = new Sensor(id, timestamp, temp);
            System.out.printf("发送数据:%s\n", sensor);
            ctx.collect(sensor);
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
