package com.lwq.bigdata.flink.stream;

/**
 * Created by Administrator on 2020-12-06.
 */
public class WordCount {
    private String word;
    private long count;

    public WordCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
