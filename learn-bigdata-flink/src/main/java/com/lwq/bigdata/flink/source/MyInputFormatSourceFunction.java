package com.lwq.bigdata.flink.source;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;

public class MyInputFormatSourceFunction extends InputFormatSourceFunction<String> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    public MyInputFormatSourceFunction(InputFormat<String, ?> format, TypeInformation<String> typeInfo) {
        super(format, typeInfo);
    }
}
