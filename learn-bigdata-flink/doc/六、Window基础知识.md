### 六、Window基础知识



#### 一、基本介绍

Apache Flink（以下简称 Flink） 是一个天然支持无限流数据处理的分布式计算框架，在 Flink 中 **Window 可以将无限流切分成有限流**，是处理有限流的核心组件，现在Flink 中 Window 可以是时间驱动的（Time Window），也可以是数据驱动的（Count Window）。下面的代码是在 Flink 中使用 Window 的两个示例 


![image-20210218212759188](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20210218212759188.png)

根据上面的示例可以看出来，window主要有以下几部分：

- windowAssigner，必需
- trigger，可选
- evictor，可选
- function，必需



#### 二、基本组件

几个基本组件的作用如下：

- **windowAssigner**：决定了数据流中的数据属于哪个window
- **trigger**：窗口触发器，决定了什么时候使用窗口函数处理窗口中的数据
- **evictor**：驱逐器，能够在触发器触发之后，窗口函数调用之前或之后从窗口中清除数据
- **function**：窗口函数



##### 大概流程

![image-20210220110903944](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20210220110903944.png)

##### 1.WindowAssinger

flink有一些内置的WindowAssigner，例如：

![image-20210218214113791](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20210218214113791.png)

比较常用的有：

- EventTimeSessionWindows
- GlobalWindows
- ProcessingTimeSessionWindows
- SlidingEventTimeSessionWindows
- SlidingProcessingTimeWindows
- TumblingEventTimeWindows
- TumblingProcessingTimeWindows

使用GlobalWindows，由于该window的默认trigger为永不触发，所以既需要实现自定义的trigger，也需要实现evictor，移除部分已经完成计算的数据。



##### 2.Trigger

trigger 指明在哪些条件下触发window计算，基于处理数据时的时间以及事件的特定属性。

- 窗口触发器，决定了窗口什么时候使用窗口函数处理窗口内元素。每个窗口分配器都带有一个默认的触发器。

- TriggerResult四个值：CONTINUE、FIRE、FIRE_AND_PURGE、PURGE

  CONTINUE：表示对窗口不执行任何操作。不触发窗口函数，不清除窗口数据

  FIRE_AND_PURGE：表示先将数据进行计算，输出结果，然后将窗口中的数据和窗口进行清除。

  FIRE：表示对窗口中的数据按照窗口函数中的逻辑进行计算，并将结果输出。注意计算完成后，**窗口中的数据并不会被清除，将会被保留**。

  PURGE：表示将窗口中的数据和窗口清除。

```java
public enum TriggerResult {

	/**
	 * No action is taken on the window.
	 */
	CONTINUE(false, false),

	/**
	 * {@code FIRE_AND_PURGE} evaluates the window function and emits the window
	 * result.
	 */
	FIRE_AND_PURGE(true, true),

	/**
	 * On {@code FIRE}, the window is evaluated and results are emitted.
	 * The window is not purged, though, all elements are retained.
	 */
	FIRE(true, false),

	/**
	 * All elements in the window are cleared and the window is discarded,
	 * without evaluating the window function or emitting any elements.
	 */
	PURGE(false, true);
}
```

- 如果后面的Function等计算用户自己增量维护状态，可以只接受增量数据则使用FIRE_AND_PURGE；
- FIRE之后的Function中会受到整个窗口的数据而FIRE_AND_PURGE只会收到增量数据，特别是在一些大窗口大数据量案例中不清理数据可能会OOM

一些内置的trigger：

![image-20210218222229741](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20210218222229741.png)

比较常用的有：

- EventTimeTrigger
- ProcessTimeTrigger
- CountTrigger
- ContinuousEventTimeTrigger
- PurgingTrigger



Trigger的接口类如下：

```java
public abstract class Trigger<T, W extends Window> implements Serializable {

	private static final long serialVersionUID == -4104633972991191369L;

	/**
	 * Called for every element that gets added to a pane. The result of this will determine
	 * whether the pane is evaluated to emit results.
	 */
	public abstract TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception;

	/**
	 * Called when a processing-time timer that was set using the trigger context fires.
	 *
	 */
	public abstract TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception;

	/**
	 * Called when an event-time timer that was set using the trigger context fires.
	 *
	 */
	public abstract TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception;

	/**
	 * Called when several windows have been merged into one window by the
	 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}.
	 *
	 * @param window The new window that results from the merge.
	 * @param ctx A context object that can be used to register timer callbacks and access state.
	 */
	public void onMerge(W window, OnMergeContext ctx) throws Exception {
		throw new UnsupportedOperationException("This trigger does not support merging.");
	}

	/**
	 * Clears any state that the trigger might still hold for the given window. This is called
	 * when a window is purged. Timers set using {@link TriggerContext#registerEventTimeTimer(long)}
	 * and {@link TriggerContext#registerProcessingTimeTimer(long)} should be deleted here as
	 * well as state acquired using {@link TriggerContext#getPartitionedState(StateDescriptor)}.
	 */
	public abstract void clear(W window, TriggerContext ctx) throws Exception;

}
```

主要的方法有：

- onElement：每个元素添加到窗口时被调用
- onProcessingTime：当一个process-time定时器被触发时调用
- onEventTime：当一个event-time定时器被触发时调用
- clear：方法会在窗口清除的时候调用



##### 3.Evictor

- Flink 窗口模型还允许在窗口分配器和触发器之外指定一个可选的驱逐器(Evictor)。
  可以使用 evictor(…) 方法来完成。
- 驱逐器能够在触发器触发之后，窗口函数使用之前或之后从窗口中清除元素。
  evictBefore()在窗口函数之前使用。而 evictAfter() 在窗口函数之后使用。
  在使用窗口函数之前被逐出的元素将不被处理。



flink提供了一些内置的Evictor：

![image-20210218223619612](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20210218223619612.png)

- CountEvictor：在窗口维护用户指定数量的元素，如果多于用户指定的数量，从窗口缓冲区的开头丢弃多余的元素。
- DeltaEvictor：使用 DeltaFunction 和一个阈值，来计算窗口缓冲区中的最后一个元素与其余每个元素之间的差值，并删除差值大于或等于阈值的元素。
- TimeEvictor：以毫秒为单位的时间间隔（interval）作为参数，对于给定的窗口，找到元素中的最大的时间戳max_ts，并删除时间戳小于max_ts - interval的所有元素。



#### 三、示例讲解

我们通过一个例子来讲解window的用法。

示例是flink官方的example中的TopSpeedWindowing。

关键代码如下：

```java
DataStream<Tuple4<Integer, Integer, Double, Long>> topSpeeds = carData
				// TODO by lwq 如何提取事件时间
				.assignTimestampsAndWatermarks(new CarTimestamp())
				// TODO by lwq 按照carId分组
				.keyBy(0)
				// TODO by lwq 指定windowAssigner
				// TODO by lwq GlobalWindows所有元素会指定到同一个window中
				.window(GlobalWindows.create())
				// TODO by lwq 指定evictor
				.evictor(TimeEvictor.of(Time.of(evictionSec, TimeUnit.SECONDS)))
				// TODO by lwq 指定trigger
				.trigger(DeltaTrigger.of(triggerMeters,
						new DeltaFunction<Tuple4<Integer, Integer, Double, Long>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public double getDelta(
									Tuple4<Integer, Integer, Double, Long> oldDataPoint,
									Tuple4<Integer, Integer, Double, Long> newDataPoint) {
								return newDataPoint.f2 - oldDataPoint.f2;
							}
						}, carData.getType().createSerializer(env.getConfig())))
				.maxBy(1);

```

1. windowAssigner为GlobalWindows，所有元素会添加到同一个window中

2. evictor为TimeEvictor，evictionSec为5s，只保留最近5s的数据，其他数据会在窗口函数执行前被删除

3. trigger为DeltaTrigger，参数有3个

   ```java
   	public static <T, W extends Window> DeltaTrigger<T, W> of(double threshold, DeltaFunction<T> deltaFunction, TypeSerializer<T> stateSerializer) {
   		return new DeltaTrigger<>(threshold, deltaFunction, stateSerializer);
   	}
   ```

   第一个是阈值，第二个是deltaFunction，第三个是输入数据的类型序列化器

   deltaFunction这里面是比较两个值的大小，也就是distance。

   再看一下trigger的onElement方法：

   ```java
   	@Override
   	public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
           // 获取lastElement的值，如果为null，更新该状态，不做任何处理
   		ValueState<T> lastElementState = ctx.getPartitionedState(stateDesc);
   		if (lastElementState.value() == null) {
   			lastElementState.update(element);
   			return TriggerResult.CONTINUE;
   		}
           // 如果lastElement不为null，调用deltaFunction比较大小，如果差值大于阈值，更新lastElement，并触发窗口函数
   		if (deltaFunction.getDelta(lastElementState.value(), element) > this.threshold) {
   			lastElementState.update(element);
   			return TriggerResult.FIRE;
   		}
   		return TriggerResult.CONTINUE;
   	}
   ```

   onElement方法主要做了两件事：

   1. 如果没有lastElement值，那么就更新这个值，执行完成

   2. 如果有这个值，调用deltaFunction，返回结果如果大于阈值，就把lastElement更新为新的element，并且触发窗口函数；

      如果不大于阈值，那么不做任何操作。



运行程序的结果如下：

```java
Executing TopSpeedWindowing example with default input data set.
Use --input to specify file input.
Printing result to stdout. Use --output to specify output path.
source: emit element:(0,55,15.277777777777777,1613786599684)
source: emit element:(1,45,12.5,1613786599692)
trigger: receive element:(0,55,15.277777777777777,1613786599684) onElement
trigger: lastElementState update first time, element:(0,55,15.277777777777777,1613786599684)
trigger: receive element:(1,45,12.5,1613786599692) onElement
trigger: lastElementState update first time, element:(1,45,12.5,1613786599692)
source: emit element:(0,60,31.944444444444443,1613786600692)
source: emit element:(1,40,23.61111111111111,1613786600692)
trigger: receive element:(0,60,31.944444444444443,1613786600692) onElement
trigger: receive element:(1,40,23.61111111111111,1613786600692) onElement
source: emit element:(0,65,50.0,1613786601692)
source: emit element:(1,45,36.111111111111114,1613786601692)
trigger: receive element:(0,65,50.0,1613786601692) onElement
trigger: receive element:(1,45,36.111111111111114,1613786601692) onElement
source: emit element:(0,60,66.66666666666667,1613786602693)
source: emit element:(1,50,50.0,1613786602693)
trigger: receive element:(0,60,66.66666666666667,1613786602693) onElement
trigger: lastElementState update, element:(0,60,66.66666666666667,1613786602693), fire
evictor: evictBefore...
result:3> (0,65,50.0,1613786601692)
trigger: receive element:(1,50,50.0,1613786602693) onElement
source: emit element:(0,65,84.72222222222223,1613786603693)
source: emit element:(1,55,65.27777777777777,1613786603693)
trigger: receive element:(0,65,84.72222222222223,1613786603693) onElement
trigger: receive element:(1,55,65.27777777777777,1613786603693) onElement
trigger: lastElementState update, element:(1,55,65.27777777777777,1613786603693), fire
evictor: evictBefore...
result:3> (1,55,65.27777777777777,1613786603693)
source: emit element:(0,60,101.3888888888889,1613786604693)
source: emit element:(1,60,81.94444444444444,1613786604693)
trigger: receive element:(0,60,101.3888888888889,1613786604693) onElement
trigger: receive element:(1,60,81.94444444444444,1613786604693) onElement
source: emit element:(0,55,116.66666666666667,1613786605693)
source: emit element:(1,55,97.22222222222221,1613786605693)
trigger: receive element:(0,55,116.66666666666667,1613786605693) onElement
trigger: receive element:(1,55,97.22222222222221,1613786605693) onElement
source: emit element:(0,50,130.55555555555557,1613786606693)
source: emit element:(1,50,111.1111111111111,1613786606693)
trigger: receive element:(0,50,130.55555555555557,1613786606693) onElement
trigger: lastElementState update, element:(0,50,130.55555555555557,1613786606693), fire
evictor: evictBefore...
result:3> (0,65,50.0,1613786601692)
trigger: receive element:(1,50,111.1111111111111,1613786606693) onElement
source: emit element:(0,55,145.83333333333334,1613786607693)
source: emit element:(1,45,123.6111111111111,1613786607693)
trigger: receive element:(0,55,145.83333333333334,1613786607693) onElement
trigger: receive element:(1,45,123.6111111111111,1613786607693) onElement
trigger: lastElementState update, element:(1,45,123.6111111111111,1613786607693), fire
evictor: evictBefore...
result:3> (1,60,81.94444444444444,1613786604693)
source: emit element:(0,60,162.5,1613786608693)
source: emit element:(1,50,137.5,1613786608693)
trigger: receive element:(0,60,162.5,1613786608693) onElement
trigger: receive element:(1,50,137.5,1613786608693) onElement
source: emit element:(0,55,177.77777777777777,1613786609693)
source: emit element:(1,55,152.77777777777777,1613786609693)
trigger: receive element:(0,55,177.77777777777777,1613786609693) onElement
trigger: receive element:(1,55,152.77777777777777,1613786609693) onElement
source: emit element:(0,50,191.66666666666666,1613786610693)
source: emit element:(1,50,166.66666666666666,1613786610693)
trigger: receive element:(0,50,191.66666666666666,1613786610693) onElement
trigger: lastElementState update, element:(0,50,191.66666666666666,1613786610693), fire
evictor: evictBefore...
evictor: evict element:(0,55,15.277777777777777,1613786599684), timestamp:1613786599684
evictor: evict element:(0,60,31.944444444444443,1613786600692), timestamp:1613786600692
result:3> (0,65,50.0,1613786601692)
```

通过结果分析可以得知：

1. 数据源**每发送一条数据都会触发trigger的onElement方法**

2. 当deltaFunction函数满足阈值时，会触发窗口函数，在本例中阈值为50，也就是当两个element的distance大于50会触发窗口函数

   日志为：lastElementState update, element:(0,55,68.05555555555554,1613785729618), fire时触发窗口函数，这时的lastElement为：lastElementState update first time, element:(0,45,12.5,1613785725607)

3. 窗口函数的输出为数据中最大的speed的那条，也就是speed为55，(0,55,68.05555555555554,1613785729618)

4. TimeEvictor的windowSize是10秒，evict方法会在窗口函数触发前执行，通过日志分析可以发现：

   trigger: receive element:(0,60,66.66666666666667,1613786602693) onElement
   trigger: lastElementState update, element:(0,60,66.66666666666667,1613786602693), fire
   evictor: evictBefore...
   result:3> (0,65,50.0,1613786601692)

   会通过trigger的onElement方法触发窗口函数，在窗口函数执行前先执行evitorBefore方法，但是不符合evitor条件所以没有清除的数据，条件就是当前窗口中所有数据的最大时间戳-windowSize（10s），就是会删除10s之前的数据。

   再看看第一次触发evcitor的日志：

   trigger: receive element:(0,50,191.66666666666666,1613786610693) onElement
   trigger: lastElementState update, element:(0,50,191.66666666666666,1613786610693), fire
   evictor: evictBefore...
   evictor: evict element:(0,55,15.277777777777777,1613786599684), timestamp:1613786599684
   evictor: evict element:(0,60,31.944444444444443,1613786600692), timestamp:1613786600692

   这时最大时间戳就是1613786610693，减去10s，就是1613786600693。

   删除的数据时间戳分别为：1613786599684和1613786600692，都小于1613786600693。



#### 四、代码

##### TopSpeedWindowing

```java
package com.lwq.bigdata.flink.window;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * An example of grouped stream windowing where different eviction and trigger
 * policies can be used. A source fetches events from cars every 100 msec
 * containing their id, their current speed (kmh), overall elapsed distance (m)
 * and a timestamp. The streaming example triggers the top speed of each car
 * every x meters elapsed for the last y seconds.
 */
public class TopSpeedWindowing {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO by lwq 事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(params);

        @SuppressWarnings({"rawtypes", "serial"})
        DataStream<Tuple4<Integer, Integer, Double, Long>> carData;
        if (params.has("input")) {
            carData = env.readTextFile(params.get("input")).map(new ParseCarData());
        } else {
            System.out.println("Executing TopSpeedWindowing example with default input data set.");
            System.out.println("Use --input to specify file input.");
            // TODO by lwq 不指定input就使用自定义输入源
            carData = env.addSource(CarSource.create(2));
        }

        int evictionSec = 10;
        double triggerMeters = 50;
        DataStream<Tuple4<Integer, Integer, Double, Long>> topSpeeds = carData
                // TODO by lwq 如何提取事件时间
                .assignTimestampsAndWatermarks(new CarTimestamp())
                // TODO by lwq 按照carId分组
                .keyBy(0)
                // TODO by lwq 指定windowAssigner
                // TODO by lwq GlobalWindows所有元素会指定到同一个window中
                .window(GlobalWindows.create())
                // TODO by lwq 指定evictor
                .evictor(MyTimeEvictor.of(Time.of(evictionSec, TimeUnit.SECONDS)))
                // TODO by lwq 指定trigger
                .trigger(MyDeltaTrigger.of(triggerMeters,
                        new DeltaFunction<Tuple4<Integer, Integer, Double, Long>>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public double getDelta(
                                    Tuple4<Integer, Integer, Double, Long> oldDataPoint,
                                    Tuple4<Integer, Integer, Double, Long> newDataPoint) {
                                return newDataPoint.f2 - oldDataPoint.f2;
                            }
                        }, carData.getType().createSerializer(env.getConfig())))
                .maxBy(1);

        if (params.has("output")) {
            topSpeeds.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            topSpeeds.print("result");
        }

        // TODO by lwq 程序核心入口
        env.execute("CarTopSpeedWindowingExample");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************
    // TODO by lwq 自定义输入源
    private static class CarSource implements SourceFunction<Tuple4<Integer, Integer, Double, Long>> {

        private static final long serialVersionUID = 1L;
        private Integer[] speeds;
        private Double[] distances;

        private Random rand = new Random();

        private volatile boolean isRunning = true;

        private CarSource(int numOfCars) {
            speeds = new Integer[numOfCars];
            distances = new Double[numOfCars];
            Arrays.fill(speeds, 50);
            Arrays.fill(distances, 0d);
        }

        public static CarSource create(int cars) {
            return new CarSource(cars);
        }

        // TODO by lwq 输出类型为Tuple4
        // TODO by lwq 第一个参数为carId，第二个参数为速度，第三个参数为距离，第四个参数为时间戳
        @Override
        public void run(SourceContext<Tuple4<Integer, Integer, Double, Long>> ctx) throws Exception {

            while (isRunning) {
                Thread.sleep(1000);
                for (int carId = 0; carId < speeds.length; carId++) {
                    if (rand.nextBoolean()) {
                        speeds[carId] = Math.min(100, speeds[carId] + 5);
                    } else {
                        speeds[carId] = Math.max(0, speeds[carId] - 5);
                    }
                    distances[carId] += speeds[carId] / 3.6d;
                    Tuple4<Integer, Integer, Double, Long> record = new Tuple4<>(carId,
                            speeds[carId], distances[carId], System.currentTimeMillis());
                    System.out.printf("source: emit element:%s\n", record);
                    ctx.collect(record);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class ParseCarData extends RichMapFunction<String, Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple4<Integer, Integer, Double, Long> map(String record) {
            String rawData = record.substring(1, record.length() - 1);
            String[] data = rawData.split(",");
            return new Tuple4<>(Integer.valueOf(data[0]), Integer.valueOf(data[1]), Double.valueOf(data[2]), Long.valueOf(data[3]));
        }
    }

    private static class CarTimestamp extends AscendingTimestampExtractor<Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(Tuple4<Integer, Integer, Double, Long> element) {
            return element.f3;
        }
    }

}

```



##### MyDeltaTrigger

```java
package com.lwq.bigdata.flink.window;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

@PublicEvolving
public class MyDeltaTrigger<T, W extends Window> extends Trigger<T, W> {
    private static final long serialVersionUID = 1L;

    private final DeltaFunction<T> deltaFunction;
    private final double threshold;
    private final ValueStateDescriptor<T> stateDesc;

    private MyDeltaTrigger(double threshold, DeltaFunction<T> deltaFunction, TypeSerializer<T> stateSerializer) {
        this.deltaFunction = deltaFunction;
        this.threshold = threshold;
        stateDesc = new ValueStateDescriptor<>("last-element", stateSerializer);

    }

    @Override
    public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
        System.out.printf("trigger: receive element:%s onElement\n", element);
        ValueState<T> lastElementState = ctx.getPartitionedState(stateDesc);
        if (lastElementState.value() == null) {
            System.out.printf("trigger: lastElementState update first time, element:%s\n", element);
            lastElementState.update(element);
            return TriggerResult.CONTINUE;
        }
        if (deltaFunction.getDelta(lastElementState.value(), element) > this.threshold) {
            System.out.printf("trigger: lastElementState update, element:%s, fire\n", element);
            lastElementState.update(element);
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(stateDesc).clear();
    }

    @Override
    public String toString() {
        return "DeltaTrigger(" + deltaFunction + ", " + threshold + ")";
    }

    /**
     * Creates a delta trigger from the given threshold and {@code DeltaFunction}.
     *
     * @param threshold       The threshold at which to trigger.
     * @param deltaFunction   The delta function to use
     * @param stateSerializer TypeSerializer for the data elements.
     * @param <T>             The type of elements on which this trigger can operate.
     * @param <W>             The type of {@link Window Windows} on which this trigger can operate.
     */
    public static <T, W extends Window> MyDeltaTrigger<T, W> of(double threshold, DeltaFunction<T> deltaFunction, TypeSerializer<T> stateSerializer) {
        return new MyDeltaTrigger<>(threshold, deltaFunction, stateSerializer);
    }
}

```



##### MyTimeEvictor

```java
package com.lwq.bigdata.flink.window;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

@PublicEvolving
public class MyTimeEvictor<W extends Window> implements Evictor<Object, W> {
    private static final long serialVersionUID = 1L;

    private final long windowSize;
    private final boolean doEvictAfter;

    public MyTimeEvictor(long windowSize) {
        this.windowSize = windowSize;
        this.doEvictAfter = false;
    }

    public MyTimeEvictor(long windowSize, boolean doEvictAfter) {
        this.windowSize = windowSize;
        this.doEvictAfter = doEvictAfter;
    }

    @Override
    public void evictBefore(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
        if (!doEvictAfter) {
            System.out.printf("evictor: evictBefore...\n");
            evict(elements, size, ctx);
        }
    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext ctx) {
        if (doEvictAfter) {
            System.out.printf("evictor: evictAfter...\n");
            evict(elements, size, ctx);
        }
    }

    private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
        if (!hasTimestamp(elements)) {
            return;
        }

        long currentTime = getMaxTimestamp(elements);
        long evictCutoff = currentTime - windowSize;

        for (Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext(); ) {
            TimestampedValue<Object> record = iterator.next();
            if (record.getTimestamp() <= evictCutoff) {
                iterator.remove();
                System.out.printf("evictor: evict element:%s, timestamp:%s\n", record.getValue(), record.getTimestamp());
            }
        }
    }

    /**
     * Returns true if the first element in the Iterable of {@link TimestampedValue} has a timestamp.
     */
    private boolean hasTimestamp(Iterable<TimestampedValue<Object>> elements) {
        Iterator<TimestampedValue<Object>> it = elements.iterator();
        if (it.hasNext()) {
            return it.next().hasTimestamp();
        }
        return false;
    }

    /**
     * @param elements The elements currently in the pane.
     * @return The maximum value of timestamp among the elements.
     */
    private long getMaxTimestamp(Iterable<TimestampedValue<Object>> elements) {
        long currentTime = Long.MIN_VALUE;
        for (Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext(); ) {
            TimestampedValue<Object> record = iterator.next();
            currentTime = Math.max(currentTime, record.getTimestamp());
        }
        return currentTime;
    }

    @Override
    public String toString() {
        return "TimeEvictor(" + windowSize + ")";
    }

    @VisibleForTesting
    public long getWindowSize() {
        return windowSize;
    }

    /**
     * Creates a {@code TimeEvictor} that keeps the given number of elements.
     * Eviction is done before the window function.
     *
     * @param windowSize The amount of time for which to keep elements.
     */
    public static <W extends Window> MyTimeEvictor<W> of(Time windowSize) {
        return new MyTimeEvictor<>(windowSize.toMilliseconds());
    }

    /**
     * Creates a {@code TimeEvictor} that keeps the given number of elements.
     * Eviction is done before/after the window function based on the value of doEvictAfter.
     *
     * @param windowSize   The amount of time for which to keep elements.
     * @param doEvictAfter Whether eviction is done after window function.
     */
    public static <W extends Window> MyTimeEvictor<W> of(Time windowSize, boolean doEvictAfter) {
        return new MyTimeEvictor<>(windowSize.toMilliseconds(), doEvictAfter);
    }
}
```



#### 五、一些问题

如何解决窗口中的乱序、迟到数据等问题？

**其解决方案就是 Watermark / allowLateNess / sideOutPut 这一组合拳。**

**Watermark** 的作用是防止 数据乱序 / 指定时间内获取不到全部数据。

**allowLateNess** 是将窗口关闭时间再延迟一段时间。

**sideOutPut **是最后兜底操作，当指定窗口已经彻底关闭后，就会把所有过期延迟数据放到侧输出流，让用户决定如何处理。

总结起来就是说

```java
Windows -----> Watermark -----> allowLateNess -----> sideOutPut 
    
用Windows把流数据分块处理，用Watermark确定什么时候不再等待更早的数据/触发窗口进行计算，用allowLateNess 将窗口关闭时间再延迟一段时间。用sideOutPut 最后兜底把数据导出到其他地方。
```