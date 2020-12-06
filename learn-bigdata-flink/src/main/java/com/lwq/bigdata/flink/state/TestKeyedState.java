package com.lwq.bigdata.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by Administrator on 2020-12-06.
 * 需求：当接收到的相同 key 的元素个数等于 3 个
 * 就计算这些元素的 value 的平均值。
 * 计算 keyed stream 中每 3 个元素的 value 的平均值
 */
public class TestKeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L),
                        Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L,
                                5L));
        dataStreamSource.keyBy(0).flatMap(new CountWindowAvgWithValueState()).print();
        env.execute(TestKeyedState.class.getSimpleName());
    }

    static class CountWindowAvgWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
        private ValueState<Tuple2<Integer, Long>> countAndSumState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 初始化状态
            countAndSumState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple2<Integer, Long>>("countAndSum", Types.TUPLE(Types.LONG, Types.INT)));
        }

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
            Tuple2<Integer, Long> countAndSum = countAndSumState.value();
            if (countAndSum == null) {
                countAndSum = Tuple2.of(0, 0L);
            }
            int count = countAndSum.f0 + 1;
            long sum = countAndSum.f1 + value.f1;
            countAndSumState.update(Tuple2.of(count, sum));
            if (count >= 3) {
                double avg = sum / count;
                countAndSumState.clear();
                out.collect(Tuple2.of(value.f0, avg));
            }
        }
    }
}
