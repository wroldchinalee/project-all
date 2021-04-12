### 七、Watermark-水印的生成方式

#### 一、基本介绍

在flink中有两种水印：

- PunctuatedWatermark：基于数据产生的水印
- PeriodicWatermark：周期性产生的水印



一般我们编写生成水印的代码模板如下：

```java
//抽取timestamp和生成watermark
	DataStream<Tuple2<String, Long>> waterMarkStream =
		inputMap.assignTimestampsAndWatermarks(
			new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
				Long currentMaxTimestamp = 0L;
				final Long maxOutOfOrderness = 10000L; // 最大允许的乱序时间是10s
			  
				@Nullable
				@Override
                //定义如何生成watermark
				public Watermark getCurrentWatermark() {
					return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
				}

				//定义如何提取timestamp
				@Override
				public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
					long timestamp = element.f1;                                        
					return timestamp;
				}
			});
```

这段代码主要做了什么？

1. 通过dataStream调用assignTimestampsAndWatermarks方法，其实就是调用了一个operator，也就是算子，生成watermark一般在source或者在source之后马上调用
2. assignTimestampsAndWatermarks的参数有两种：AssignerWithPeriodicWatermarks和AssignerWithPunctuatedWatermarks，对应两种水印。
3. 示例中我们自定义了一个周期性的水印生成器，getCurrentWatermark定义了如何生成Watermark，当然这段代码有些问题，因为这个水印产生不是递增的，extractTimestamp定义了如何提取timestamp

#### 二、生成方式

前面讲到assignTimestampsAndWatermarks方法有两个参数，根据不同的参数，我们看一下两个方法

##### 第一个：AssignerWithPunctuatedWatermarks

```java
	public SingleOutputStreamOperator<T> assignTimestampsAndWatermarks(
			AssignerWithPunctuatedWatermarks<T> timestampAndWatermarkAssigner) {

		// match parallelism to input, otherwise dop=1 sources could lead to some strange
		// behaviour: the watermark will creep along very slowly because the elements
		// from the source go to each extraction operator round robin.
		final int inputParallelism = getTransformation().getParallelism();
		final AssignerWithPunctuatedWatermarks<T> cleanedAssigner = clean(timestampAndWatermarkAssigner);

		TimestampsAndPunctuatedWatermarksOperator<T> operator =
				new TimestampsAndPunctuatedWatermarksOperator<>(cleanedAssigner);

		return transform("Timestamps/Watermarks", getTransformation().getOutputType(), operator)
				.setParallelism(inputParallelism);
	}
```

再具体看一下这个算子的代码：

```java
public void processElement(StreamRecord<T> element) throws Exception {
	final T value = element.getValue();
	// 通过用户的代码获取到事件时间，注入到element里面就直接往下个opeartor发送
	final long newTimestamp = userFunction.extractTimestamp(value,
			element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE);

    // 替换element中的时间戳，并将数据发送到下游
	output.collect(element.replace(element.getValue(), newTimestamp));
	// 通过用户代码获取水印
	final Watermark nextWatermark = userFunction.checkAndGetNextWatermark(value, newTimestamp);

    // 判断是不是null，如果不为null并且大于现在的水印，发送到下游
	if (nextWatermark != null && nextWatermark.getTimestamp() > currentWatermark) {
		currentWatermark = nextWatermark.getTimestamp();
		output.emitWatermark(nextWatermark);
	}
}
```

先看一下方法的参数，核心代码如下：

```java
public final class StreamRecord<T> extends StreamElement {

	/** The actual value held by this record. */
	private T value;

	/** The timestamp of the record. */
	private long timestamp;

	/** Flag whether the timestamp is actually set. */
	private boolean hasTimestamp;
}
```

其实就是封装了数据流中元素的值和时间戳。

整个方法主要内容就是：

1. 调用用户代码extractTimestamp获取时间戳
2. 将elment的时间戳替换为新的时间戳并将element发送到下游
3. 调用用户代码getCurrentWatermark获取水印
4. 判断水印符合条件发送水印



##### 第二个：AssignerWithPeriodicWatermarks

```java
	public SingleOutputStreamOperator<T> assignTimestampsAndWatermarks(
			AssignerWithPeriodicWatermarks<T> timestampAndWatermarkAssigner) {

		// match parallelism to input, otherwise dop=1 sources could lead to some strange
		// behaviour: the watermark will creep along very slowly because the elements
		// from the source go to each extraction operator round robin.
		final int inputParallelism = getTransformation().getParallelism();
		final AssignerWithPeriodicWatermarks<T> cleanedAssigner = clean(timestampAndWatermarkAssigner);

		TimestampsAndPeriodicWatermarksOperator<T> operator =
				new TimestampsAndPeriodicWatermarksOperator<>(cleanedAssigner);

		return transform("Timestamps/Watermarks", getTransformation().getOutputType(), operator)
				.setParallelism(inputParallelism);
	}
```

这个方法主要是创建了一个TimestampsAndPeriodicWatermarksOperator，然后执行这个算子。

这个算子的具体代码如下：

```java
	public void open() throws Exception {
		super.open();

		currentWatermark = Long.MIN_VALUE;
		watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
		
        // 注册一个处理时间定时器
		if (watermarkInterval > 0) {
			long now = getProcessingTimeService().getCurrentProcessingTime();
			getProcessingTimeService().registerTimer(now + watermarkInterval, this);
		}
	}
```

open方法主要就是通过配置获取产生水印的时间间隔，然后注册一个处理时间的定时器。

```java
	@Override
	public void processElement(StreamRecord<T> element) throws Exception {
		final long newTimestamp = userFunction.extractTimestamp(element.getValue(),
				element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE);

		output.collect(element.replace(element.getValue(), newTimestamp));
	}
```

processElement调用用户代码，提取时间戳，并将element中的时间戳更新，然后将element发送到下游。

```java
	public void onProcessingTime(long timestamp) throws Exception {
		// register next timer
		Watermark newWatermark = userFunction.getCurrentWatermark();
		if (newWatermark != null && newWatermark.getTimestamp() > currentWatermark) {
			currentWatermark = newWatermark.getTimestamp();
			// emit watermark
			output.emitWatermark(newWatermark);
		}

		long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(now + watermarkInterval, this);
	}
```

onProcessingTime方法主要做了：

1. 调用用户代码生成水印
2. 如果新生成水印不为null，并且大于当前的水印，发送水印
3. 注册一个定时器



再看一下发送的水印具体是什么？

代码：

```java
public final class Watermark extends StreamElement {

	/** The watermark that signifies end-of-event-time. */
	public static final Watermark MAX_WATERMARK = new Watermark(Long.MAX_VALUE);

	// ------------------------------------------------------------------------

	/** The timestamp of the watermark in milliseconds. */
	private final long timestamp;

	/**
	 * Creates a new watermark with the given timestamp in milliseconds.
	 */
	public Watermark(long timestamp) {
		this.timestamp = timestamp;
	}
}
```

可以看到，水印的本质其实就是一个时间戳。



##### 总结

可以看出来两种方式主要区别：

第一种是每处理一条数据，只要符合条件，就发送一条水印。

第二种是通过主要定时器的方式，每过一段时间发送一条水印，然后再注册定时器。



#### 三、代码案例

代码：

```java
public class WatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 5555);
        // 输入格式为: word,timestamp
        // timestamp为yyyy-MM-dd HH:mm:ss格式的字符串
        SingleOutputStreamOperator<String> resultStream = inputStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                if (StringUtils.isEmpty(value)) {
                    return null;
                }
                String[] split = value.split(",");
                return Tuple2.of(split[0], split[1]);
            }
        }).assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks(5))
                // 转换为word,count格式
                .map(new MapFunction<Tuple2<String, String>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, String> value) throws Exception {
                        return Tuple2.of(value.f0, 1L);
                    }
                })
                // 按照word分组
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                // 创建timeWindow
                .timeWindow(Time.seconds(10), Time.seconds(5))
                // 窗口函数，触发时计算当前窗口单词计数，并打印所属窗口的开始时间和结束时间
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        String windowStart = simpleDateFormat.format(new Date(context.window().getStart()));
                        String windowEnd = simpleDateFormat.format(new Date(context.window().getEnd()));
                        long count = 0;
                        for (Tuple2<String, Long> element : elements) {
                            count += element.f1;
                        }
                        String result = String.format("word:%s,count:%d,windowStart:%s,windowEnd:%s", s, count, windowStart, windowEnd);
                        out.collect(result);
                    }
                });
        resultStream.print();
        env.execute("watermark demo");
    }

    static class MyAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Tuple2<String, String>> {
        private int maxOutOfOrderness; // 秒
        private long currMaxTimestamp; // 最大时间戳
        private long lastEmittedWatermark; // 上次生成的水印
        private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        public MyAssignerWithPeriodicWatermarks(int maxOutOfOrderness) {
            this.maxOutOfOrderness = maxOutOfOrderness;
        }

        /**
         * 生成水印，如果当前最大时间戳 - 允许迟到最大时间 > 上次发送的时间戳就更新上次发送的时间戳
         * 然后生成时间戳
         * @return
         */
        @Override
        public Watermark getCurrentWatermark() {
            long temp = currMaxTimestamp - maxOutOfOrderness * 1000;
            if (temp > lastEmittedWatermark) {
                lastEmittedWatermark = temp;
            }
            return new Watermark(lastEmittedWatermark);
        }

        @Override
        public long extractTimestamp(Tuple2<String, String> element, long previousElementTimestamp) {
            long timestamp = toMills(element.f1);
            if (timestamp > currMaxTimestamp) {
                currMaxTimestamp = timestamp;
            }
            return timestamp;
        }

        private long toMills(String formatDt) {
            try {
                Date date = simpleDateFormat.parse(formatDt);
                return date.getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return -1;
        }
    }
}

```

输入数据：

> hello,2021-02-23 22:30:00
> hello,2021-02-23 22:30:01
> hello,2021-02-23 22:30:02
> hello,2021-02-23 22:30:03
> hello,2021-02-23 22:30:04
> hello,2021-02-23 22:30:05
> hello,2021-02-23 22:30:05
> hello,2021-02-23 22:30:06
> hello,2021-02-23 22:30:07
> hello,2021-02-23 22:30:08
> hello,2021-02-23 22:30:09
> hello,2021-02-23 22:30:10
> hello,2021-02-23 22:30:11
> hello,2021-02-23 22:30:12
> hello,2021-02-23 22:30:14
> hello,2021-02-23 22:30:15
> hello,2021-02-23 22:30:16
> hello,2021-02-23 22:30:17
> hello,2021-02-23 22:30:18
> hello,2021-02-23 22:30:19
> hello,2021-02-23 22:30:20

输出数据：

> word:hello,count:5,windowStart:2021-02-23 22:29:55,windowEnd:2021-02-23 22:30:05
> word:hello,count:11,windowStart:2021-02-23 22:30:00,windowEnd:2021-02-23 22:30:10
> word:hello,count:10,windowStart:2021-02-23 22:30:05,windowEnd:2021-02-23 22:30:15



根据输入和输出数据分析：

第一行输出的窗口范围为：22:29:55-22:30:05，窗口大小为10s，是通过hello,2021-02-23 22:30:10这条数据触发的，这时候的水印为

22:30:10-5=22:30:05，表示22:30:05之前的数据（不包括22:30:05）已经来到了，计算结果为5。

第二行输出的窗口范围为：22:30:00-22:30:10，窗口大小为10s，是通过hello,2021-02-23 22:30:15这条数据触发的，这时候的水印为

22:30:15-5=22:30:10，表示22:30:10之前的数据（不包括22:30:10）已经来到了，计算结果为11。

第三行输出的窗口范围为：22:30:05-22:30:15，窗口大小为10s，是通过hello,2021-02-23 22:30:20这条数据触发的，这时候的水印为

22:30:,20-5=22:30:15，表示22:30:15之前的数据（不包括22:30:15）已经来到了，计算结果为20。