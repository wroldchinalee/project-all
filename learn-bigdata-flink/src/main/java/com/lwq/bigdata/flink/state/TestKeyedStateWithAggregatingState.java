package com.lwq.bigdata.flink.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by Administrator on 2020-12-09.
 */
public class TestKeyedStateWithAggregatingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L),
                        Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L,
                                5L));
        dataStreamSource.keyBy(0).flatMap(new TestKeyedStateWithAggregatingState.ContainsValueFunction()).print();
        env.execute(TestKeyedStateWithAggregatingState.class.getSimpleName());
    }

    static class ContainsValueFunction extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, String>> {
        private AggregatingState<Long, String> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            AggregatingStateDescriptor<Long, String, String> stateDescriptor = new AggregatingStateDescriptor<>("state", new AggregateFunction<Long, String, String>() {
                @Override
                public String createAccumulator() {
                    return "Contains: ";
                }

                @Override
                public String add(Long value, String accumulator) {
                    String ret;
                    if (accumulator.equals("Contains: ")) {
                        ret = accumulator + value;
                    } else {
                        ret = accumulator + " and " + value;
                    }
                    return ret;
                }

                @Override
                public String getResult(String accumulator) {
                    return accumulator;
                }

                @Override
                public String merge(String a, String b) {
                    return a + b;
                }
            }, Types.STRING);
            this.state = getRuntimeContext().getAggregatingState(stateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, String>> out) throws Exception {
            state.add(value.f1);
            out.collect(Tuple2.of(value.f0, state.get()));
        }
    }
}
