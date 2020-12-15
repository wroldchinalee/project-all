package com.lwq.bigdata.flink.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by Administrator on 2020-12-08.
 */
public class TestKeyedStateWithReducingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
            env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L),
                    Tuple2.of(1L, 7L),
                    Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L,
                            5L));
        dataStreamSource.keyBy(0).flatMap(new SumFunction()).print();
        env.execute(TestKeyedStateWithListState.class.getSimpleName());
}

    static class SumFunction extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
        private ReducingState<Long> reducingState;


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ReducingStateDescriptor<Long> stateDescriptor = new ReducingStateDescriptor<>("state", new ReduceFunction<Long>() {
                @Override
                public Long reduce(Long value1, Long value2) throws Exception {
                    return value1 + value2;
                }
            }, Types.LONG);
            this.reducingState = getRuntimeContext().getReducingState(stateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
            reducingState.add(value.f1);
            out.collect(Tuple2.of(value.f0, reducingState.get()));
        }
    }
}
