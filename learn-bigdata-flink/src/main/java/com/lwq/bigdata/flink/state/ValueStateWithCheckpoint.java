package com.lwq.bigdata.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 数据为vswc.text
 */
public class ValueStateWithCheckpoint {
    private static Logger LOGGER = LoggerFactory.getLogger(ValueStateWithCheckpoint.class);

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "master");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://bigdata01:9000/users/master/flink/checkpoints"));

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 5555);
        inputStream.map(new MapFunction<String, Tuple3<Integer, Long, Integer>>() {
            @Override
            public Tuple3<Integer, Long, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple3.of(Integer.parseInt(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        }).process(new MyProcessFunction()).keyBy(0).sum(1).print("result:");
        env.execute("ValueStateWithCheckpoint");
    }

    // 记录当前消费的位移状态
    static class MyProcessFunction extends ProcessFunction<Tuple3<Integer, Long, Integer>, Tuple2<Integer, Integer>> implements CheckpointedFunction {
        private Map<Integer, Long> partition2Offsets;
        private ListState<Tuple2<Integer, Long>> partition2OffsetUnionListState;

        @Override

        public void processElement(Tuple3<Integer, Long, Integer> value, Context ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            LOGGER.info("MyProcessFunction processElement");
            // 更新offset
            partition2Offsets.put(value.f0, value.f1);
            // 更新count
            String result = String.format("partition:%s,offset:%s", value.f0, value.f1);
            LOGGER.info(result);
            out.collect(Tuple2.of(value.f0, value.f2));
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            if (partition2Offsets == null) {
                partition2Offsets = new HashMap<>();
            }
            LOGGER.info("MyProcessFunction open");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            partition2OffsetUnionListState.clear();
            for (Map.Entry<Integer, Long> entry : partition2Offsets.entrySet()) {
                partition2OffsetUnionListState.add(Tuple2.of(entry.getKey(), entry.getValue()));
                LOGGER.info("MyProcessFunction snapshotState partition:{},offset:{}", entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            LOGGER.info("subtask:{} MyProcessFunction initializeState", getRuntimeContext().getIndexOfThisSubtask());
            ListStateDescriptor<Tuple2<Integer, Long>> offsetUnionListState = new ListStateDescriptor<>("offsetUnionListState", Types.TUPLE(Types.INT, Types.LONG));
            partition2OffsetUnionListState = context.getOperatorStateStore().getUnionListState(offsetUnionListState);
            if (context.isRestored()) {
                Iterator<Tuple2<Integer, Long>> iterator = partition2OffsetUnionListState.get().iterator();
                partition2Offsets = new HashMap<>();
                while (iterator.hasNext()) {
                    Tuple2<Integer, Long> next = iterator.next();
                    partition2Offsets.put(next.f0, next.f1);
                    LOGGER.info("partition:{},offset:{} in Restore", next.f0, next.f1);
                }

                LOGGER.info("subtask:%s initializeState successful inRestore", getRuntimeContext().getIndexOfThisSubtask());
            } else {
                LOGGER.info("subtask:{} initializeState successful", getRuntimeContext().getIndexOfThisSubtask());
            }

        }
    }
}
