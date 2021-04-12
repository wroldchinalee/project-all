package com.lwq.bigdata.flink.window;

import com.lwq.bigdata.flink.mix.MySource1;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 实时计算单词出现的次数，但是并不是每次接受到单词以后就输出单词出现的次数，⽽是当过了5秒以后没
 * 收到这个单词，就输出这个单词的次数
 */
public class MyWindowTest1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputSource = env.addSource(new MySource1());
        SingleOutputStreamOperator<String> streamOperator = inputSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                for (int i = 0; i < split.length; i++) {
                    out.collect(Tuple2.of(split[i], 1L));
                }
            }
        }).keyBy(0).process(new CountWithTimeFunction());
        streamOperator.print();

        env.execute(MyWindowTest1.class.getSimpleName());
    }

    private static class CountWithTimeFunction extends KeyedProcessFunction<Tuple, Tuple2<String, Long>, String> {
        private ValueState<CountWithTime> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            state = getRuntimeContext().getState(new ValueStateDescriptor<CountWithTime>("state", CountWithTime.class));
        }

        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
            CountWithTime countWithTime = state.value();
            long now = ctx.timerService().currentProcessingTime();
            if (countWithTime == null) {
                countWithTime = new CountWithTime(value.f0, 1, now);
            } else {
                countWithTime.setCount(countWithTime.count + 1);
                countWithTime.setLastCountTime(now);
            }

            state.update(countWithTime);
            ctx.timerService().registerProcessingTimeTimer(now + 5000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            CountWithTime countWithTime = state.value();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            if (timestamp == countWithTime.lastCountTime + 5000) {
                String format1 = simpleDateFormat.format(new Date(countWithTime.lastCountTime));
                String format2 = simpleDateFormat.format(new Date(timestamp));
                String s = String.format("word:%s,count:%s,lastCountTime:%s,currTime:%s",
                        countWithTime.word, countWithTime.count, format1, format2);
                out.collect(s);
                state.clear();
            } else {
                String format = simpleDateFormat.format(new Date(timestamp));
                String s = String.format("触发了定时器:%s", format);
                out.collect(s);
            }
        }
    }

    private static class CountWithTime {
        private String word;
        private long count;
        private long lastCountTime;

        public CountWithTime(String word, long count, long lastCountTime) {
            this.word = word;
            this.count = count;
            this.lastCountTime = lastCountTime;
        }


        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public void setLastCountTime(long lastCountTime) {
            this.lastCountTime = lastCountTime;
        }

        public long getLastCountTime() {
            return lastCountTime;
        }
    }
}
