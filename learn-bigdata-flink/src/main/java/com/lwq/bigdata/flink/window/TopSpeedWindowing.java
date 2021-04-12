package com.lwq.bigdata.flink.window;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * An example of grouped stream windowing where different eviction and trigger
 * policies can be used. A source fetches events from cars every 100 msec
 * containing their id, their current speed (kmh), overall elapsed distance (m)
 * and a timestamp. The streaming example triggers the top speed of each car
 * every x meters elapsed for the last y seconds.
 */
public class TopSpeedWindowing {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO by lwq 事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(params);

        @SuppressWarnings({"rawtypes", "serial"})
        DataStream<Tuple4<Integer, Integer, Double, Long>> carData;
        if (params.has("input")) {
            carData = env.readTextFile(params.get("input")).map(new ParseCarData());
        } else {
            System.out.println("Executing TopSpeedWindowing example with default input data set.");
            System.out.println("Use --input to specify file input.");
            // TODO by lwq 不指定input就使用自定义输入源
            carData = env.addSource(CarSource.create(2));
        }

        int evictionSec = 10;
        double triggerMeters = 50;
        DataStream<Tuple4<Integer, Integer, Double, Long>> topSpeeds = carData
                // TODO by lwq 如何提取事件时间
                .assignTimestampsAndWatermarks(new CarTimestamp())
                // TODO by lwq 按照carId分组
                .keyBy(0)
                // TODO by lwq 指定windowAssigner
                // TODO by lwq GlobalWindows所有元素会指定到同一个window中
                .window(GlobalWindows.create())
                // TODO by lwq 指定evictor
                .evictor(MyTimeEvictor.of(Time.of(evictionSec, TimeUnit.SECONDS)))
                // TODO by lwq 指定trigger
                .trigger(MyDeltaTrigger.of(triggerMeters,
                        new DeltaFunction<Tuple4<Integer, Integer, Double, Long>>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public double getDelta(
                                    Tuple4<Integer, Integer, Double, Long> oldDataPoint,
                                    Tuple4<Integer, Integer, Double, Long> newDataPoint) {
                                return newDataPoint.f2 - oldDataPoint.f2;
                            }
                        }, carData.getType().createSerializer(env.getConfig())))
                .maxBy(1);

        if (params.has("output")) {
            topSpeeds.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            topSpeeds.print("result");
        }

        // TODO by lwq 程序核心入口
        env.execute("CarTopSpeedWindowingExample");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************
    // TODO by lwq 自定义输入源
    private static class CarSource implements SourceFunction<Tuple4<Integer, Integer, Double, Long>> {

        private static final long serialVersionUID = 1L;
        private Integer[] speeds;
        private Double[] distances;

        private Random rand = new Random();

        private volatile boolean isRunning = true;

        private CarSource(int numOfCars) {
            speeds = new Integer[numOfCars];
            distances = new Double[numOfCars];
            Arrays.fill(speeds, 50);
            Arrays.fill(distances, 0d);
        }

        public static CarSource create(int cars) {
            return new CarSource(cars);
        }

        // TODO by lwq 输出类型为Tuple4
        // TODO by lwq 第一个参数为carId，第二个参数为速度，第三个参数为距离，第四个参数为时间戳
        @Override
        public void run(SourceContext<Tuple4<Integer, Integer, Double, Long>> ctx) throws Exception {

            while (isRunning) {
                Thread.sleep(1000);
                for (int carId = 0; carId < speeds.length; carId++) {
                    if (rand.nextBoolean()) {
                        speeds[carId] = Math.min(100, speeds[carId] + 5);
                    } else {
                        speeds[carId] = Math.max(0, speeds[carId] - 5);
                    }
                    distances[carId] += speeds[carId] / 3.6d;
                    Tuple4<Integer, Integer, Double, Long> record = new Tuple4<>(carId,
                            speeds[carId], distances[carId], System.currentTimeMillis());
                    System.out.printf("source: emit element:%s\n", record);
                    ctx.collect(record);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class ParseCarData extends RichMapFunction<String, Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple4<Integer, Integer, Double, Long> map(String record) {
            String rawData = record.substring(1, record.length() - 1);
            String[] data = rawData.split(",");
            return new Tuple4<>(Integer.valueOf(data[0]), Integer.valueOf(data[1]), Double.valueOf(data[2]), Long.valueOf(data[3]));
        }
    }

    private static class CarTimestamp extends AscendingTimestampExtractor<Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(Tuple4<Integer, Integer, Double, Long> element) {
            return element.f3;
        }
    }

}
