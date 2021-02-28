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