package com.lwq.bigdata.flink.state;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Created by Administrator on 2020-12-08.
 */
public class TestKeyedStateWithListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L),
                        Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L,
                                5L));
        dataStreamSource.keyBy(0).flatMap(new CountWindowAvgWithValueState(3)).print();
        env.execute(TestKeyedStateWithListState.class.getSimpleName());
    }

    /**
     * ListState<T> ：这个状态为每一个 key 保存集合的值
     * get() 获取状态值
     * add() / addAll() 更新状态值，将数据放到状态中
     * clear() 清除状态
     */
    static class CountWindowAvgWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
        private int count;

        public CountWindowAvgWithValueState(int count) {
            this.count = count;
        }

        private ListState<Long> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 注册状态
            this.listState = getRuntimeContext().getListState(new ListStateDescriptor<>("listState", Types.LONG));
        }

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
            // 如果状态值为空，初始化一下
            listState.add(value.f1);
            ArrayList<Long> list = Lists.newArrayList(listState.get().iterator());
            if (list.size() >= count) {
                long sum = 0;
                for (int i = 0; i < list.size(); i++) {
                    sum += list.get(i);
                }
                double avg = sum * 1.0 / list.size();
                listState.clear();
                out.collect(Tuple2.of(value.f0, avg));
            }
        }
    }
}



