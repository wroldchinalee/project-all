package com.lwq.bigdata.flink.window;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class WatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 5555);
        // 输入格式为: word,timestamp
        // timestamp为yyyy-MM-dd HH:mm:ss格式的字符串
        SingleOutputStreamOperator<String> resultStream = inputStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                if (StringUtils.isEmpty(value)) {
                    return null;
                }
                String[] split = value.split(",");
                return Tuple2.of(split[0], split[1]);
            }
        }).assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks(5))
                // 转换为word,count格式
                .map(new MapFunction<Tuple2<String, String>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, String> value) throws Exception {
                        return Tuple2.of(value.f0, 1L);
                    }
                })
                // 按照word分组
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                // 创建timeWindow
                .timeWindow(Time.seconds(10), Time.seconds(5))
                // 窗口函数，触发时计算当前窗口单词计数，并打印所属窗口的开始时间和结束时间
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        String windowStart = simpleDateFormat.format(new Date(context.window().getStart()));
                        String windowEnd = simpleDateFormat.format(new Date(context.window().getEnd()));
                        long count = 0;
                        for (Tuple2<String, Long> element : elements) {
                            count += element.f1;
                        }
                        String result = String.format("word:%s,count:%d,windowStart:%s,windowEnd:%s", s, count, windowStart, windowEnd);
                        out.collect(result);
                    }
                });
        resultStream.print();
        env.execute("watermark demo");
    }

    static class MyAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Tuple2<String, String>> {
        private int maxOutOfOrderness; // 秒
        private long currMaxTimestamp; // 最大时间戳
        private long lastEmittedWatermark; // 上次生成的水印
        private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        public MyAssignerWithPeriodicWatermarks(int maxOutOfOrderness) {
            this.maxOutOfOrderness = maxOutOfOrderness;
        }

        /**
         * 生成水印，如果当前最大时间戳 - 允许迟到最大时间 > 上次发送的时间戳就更新上次发送的时间戳
         * 然后生成时间戳
         * @return
         */
        @Override
        public Watermark getCurrentWatermark() {
            long temp = currMaxTimestamp - maxOutOfOrderness * 1000;
            if (temp > lastEmittedWatermark) {
                lastEmittedWatermark = temp;
            }
            return new Watermark(lastEmittedWatermark);
        }

        @Override
        public long extractTimestamp(Tuple2<String, String> element, long previousElementTimestamp) {
            long timestamp = toMills(element.f1);
            if (timestamp > currMaxTimestamp) {
                currMaxTimestamp = timestamp;
            }
            return timestamp;
        }

        private long toMills(String formatDt) {
            try {
                Date date = simpleDateFormat.parse(formatDt);
                return date.getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return -1;
        }
    }
}
