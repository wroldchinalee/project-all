package com.lwq.bigdata.flink.stream;

import com.lwq.bigdata.flink.source.MyNoParallelSource;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * Created by Administrator on 2020-12-06.
 * 把一个数据流切分成多个数据流
 * 应用场景：
 * 可能在实际工作中，源数据流中混合了多种类似的数据，多种类型的数据处理规则不一样，所以就可以在
 * 根据一定的规则，
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不用的处理逻辑了
 */
public class SplitDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> streamSource = environment.addSource(new MyNoParallelSource());
        SplitStream<Long> splitStream = streamSource.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> list = new ArrayList<>();
                if (value % 2 == 0) {
                    list.add("even");
                } else {
                    list.add("odd");
                }
                return list;
            }
        });

        DataStream<Long> oddStream = splitStream.select("odd");
        DataStream<Long> evenStream = splitStream.select("even");
        DataStream<Long> moreStream = splitStream.select("odd", "even");

        evenStream.print();
        environment.execute(SplitDemo.class.getSimpleName());

    }
}
