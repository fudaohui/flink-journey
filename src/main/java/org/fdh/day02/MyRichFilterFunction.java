package org.fdh.day02;

import org.apache.flink.api.common.functions.RichFilterFunction;

public class MyRichFilterFunction extends RichFilterFunction<Integer> {

    @Override
    public boolean filter(Integer value) throws Exception {
        return value >= 10;
    }

}
