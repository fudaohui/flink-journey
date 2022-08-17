package org.fdh.day02;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * map 映射转换，一个输入对应一个输出
 */
public class DoubleMapFunction implements MapFunction<Integer, String> {
    @Override
    public String map(Integer value) throws Exception {
        return "function input is:" + value + ",output is:" + value * 2;
    }
}
