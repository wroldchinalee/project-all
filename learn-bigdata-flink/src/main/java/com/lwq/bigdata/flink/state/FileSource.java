package com.lwq.bigdata.flink.state;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2020-12-10.
 */
public class FileSource implements SourceFunction<String> {
    private String filePath;

    public FileSource(String filePath) {
        this.filePath = filePath;
    }

    private BufferedReader bufferedReader;
    private InputStream inputStream;
    private Random random = new Random();

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        inputStream = new FileInputStream(filePath);
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            TimeUnit.MILLISECONDS.sleep(random.nextInt(5000));
            ctx.collect(line);
        }

        if (bufferedReader != null) {
            bufferedReader.close();
        }
        if (inputStream != null) {
            inputStream.close();
        }
    }

    @Override
    public void cancel() {
        try {
            if (bufferedReader != null) {
                bufferedReader.close();
            }

            if (inputStream != null) {
                inputStream.close();
            }
        } catch (Exception e) {
        }
    }
}
