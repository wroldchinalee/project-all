package com.lwq.bigdata.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.UUID;

/**
 * Created by Administrator on 2020-12-08.
 */
public class TestKeyedStateWithMapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L),
                        Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L,
                                5L));
        dataStreamSource.keyBy(0).flatMap(new CountWindowAvgWithMapState(3)).print();
        env.execute(TestKeyedStateWithListState.class.getSimpleName());
    }

    /**
     * MapState<K, V> ：这个状态为每一个 key 保存一个 Map 集合
     * put() 将对应的 key 的键值对放到状态中
     * values() 拿到 MapState 中所有的 value
     * clear() 清除状态
     */
    static class CountWindowAvgWithMapState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
        private int COUNT;
        //1. MapState ：key 是一个唯一的值，value 是接收到的相同的 key 对应的 value 的值
        private MapState<String, Long> mapState;

        public CountWindowAvgWithMapState(int COUNT) {
            this.COUNT = COUNT;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 注册状态
            this.mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapState", Types.STRING, Types.LONG));
        }

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
            String uuid = UUID.randomUUID().toString();
            mapState.put(uuid, value.f1);
            ArrayList<Long> values = Lists.newArrayList(mapState.values());
            if (values.size() >= COUNT) {
                long sum = 0;
                for (Long temp : values) {
                    sum += temp;
                }
                double avg = (double) sum / values.size();
                mapState.clear();
                out.collect(Tuple2.of(value.f0, avg));
            }
        }
    }
}
