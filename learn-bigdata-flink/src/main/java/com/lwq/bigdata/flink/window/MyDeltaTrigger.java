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
