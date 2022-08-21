package org.fdh.day05;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.fdh.pojo.StockPrice;

import java.util.Iterator;

/**
 * 处理时间窗口内的开盘价，最高价，最低价，收盘价
 * note:ProcessWindowFunction 要缓存窗口里面的所有数据，数据量比较多
 */
public class OHLCProcessFunction extends ProcessWindowFunction<StockPrice, Tuple5<String, Double, Double, Double, Double>, String, TimeWindow> {

    @Override
    public void process(String s
            , ProcessWindowFunction<StockPrice, Tuple5<String, Double, Double, Double, Double>, String, TimeWindow>.Context context
            , Iterable<StockPrice> elements
            , Collector<Tuple5<String, Double, Double, Double, Double>> out) throws Exception {
        Iterator<StockPrice> iterator = elements.iterator();

        //没有数据需要返回
        if (iterator.hasNext()) {
            Double open = iterator.next().price;
            Double high = open;
            Double low = open;
            Double close = open;
            for (StockPrice element : elements) {
                if (high < element.price) {
                    high = element.price;
                }
                if (low > element.price) {
                    low = element.price;
                }
                close = element.price;
            }
            out.collect(Tuple5.of(s, open, high, low, close));
        }

    }
}
