package com.lwq.bigdata.flink.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2020-12-09.
 * 实现一个需求：
 * 正常的DataStream.print方法是每来一条数据，就打印一次
 * 现在需求是指定一个阈值，达到阈值就打印累积的数据
 * <p>
 * 分析：
 * 通过OperatorState来保存数据，达到阈值时打印
 */
public class CustomSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {
    // 先保存到内存
    private List<Tuple2<String, Integer>> memoryList;
    private ListState<Tuple2<String, Integer>> listState;
    // 阈值
    private int threshold;

    public CustomSink(int threshold) {
        this.threshold = threshold;
        this.memoryList = new ArrayList<>();
    }

    // 这个是SinkFunction的方法
    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        memoryList.add(value);
        // 达到阈值打印
        if (memoryList.size() >= threshold) {
            // 简单打印
            System.out.println("自定义:" + memoryList);
            memoryList.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        listState.clear();
        for (Tuple2<String, Integer> tuple2 : memoryList) {
            listState.add(tuple2);
        }
    }

    // 初始化状态
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> stateDescriptor = new ListStateDescriptor<>("listState", Types.TUPLE(Types.STRING, Types.INT));
        this.listState = context.getOperatorStateStore().getListState(stateDescriptor);

        if (context.isRestored()) {
            for (Tuple2<String, Integer> tuple2 : listState.get()) {
                memoryList.add(tuple2);
            }
        }
    }
}
