package com.lwq.bigdata.flink.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Created by Administrator on 2020-12-10.
 */
public class OrderStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> info1 = env.addSource(new
                FileSource(Constants.ORDER_INFO1_PATH));
        DataStreamSource<String> info2 = env.addSource(new
                FileSource(Constants.ORDER_INFO2_PATH));

        KeyedStream<OrderInfo1, Long> orderInfo1Stream = info1.map(line ->
                OrderInfo1.string2OrderInfo1(line))
                .keyBy(orderInfo1 -> orderInfo1.getOrderId());
        KeyedStream<OrderInfo2, Long> orderInfo2Stream = info2.map(line ->
                OrderInfo2.string2OrderInfo2(line))
                .keyBy(orderInfo2 -> orderInfo2.getOrderId());

        orderInfo1Stream.connect(orderInfo2Stream)
                .flatMap(new EnrichmentFunction())
                .print();
        env.execute("OrderStream");
    }

    /**
     * IN1, 第一个流的输入的数据类型
     * IN2, 第二个流的输入的数据类型
     * OUT，输出的数据类型
     */
    public static class EnrichmentFunction extends RichCoFlatMapFunction<OrderInfo1, OrderInfo2, Tuple2<OrderInfo1, OrderInfo2>> {
        // 定义第一个流key对应的state
        private ValueState<OrderInfo1> orderInfo1ValueState;
        // 定义第二个流key对应的state
        private ValueState<OrderInfo2> orderInfo2ValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            orderInfo1ValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("order1State", OrderInfo1.class));
            orderInfo2ValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("order2State", OrderInfo2.class));
        }

        @Override
        public void flatMap1(OrderInfo1 value, Collector<Tuple2<OrderInfo1, OrderInfo2>> out) throws Exception {
            OrderInfo2 orderInfo2 = orderInfo2ValueState.value();
            if (orderInfo2 != null) {
                orderInfo2ValueState.clear();
                out.collect(Tuple2.of(value, orderInfo2));
            } else {
                orderInfo1ValueState.update(value);
            }
        }

        @Override
        public void flatMap2(OrderInfo2 value, Collector<Tuple2<OrderInfo1, OrderInfo2>> out) throws Exception {
            OrderInfo1 orderInfo1 = orderInfo1ValueState.value();
            if (orderInfo1 != null) {
                orderInfo1ValueState.clear();
                out.collect(Tuple2.of(orderInfo1, value));
            } else {
                orderInfo2ValueState.update(value);
            }
        }
    }
}


