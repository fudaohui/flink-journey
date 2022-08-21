package org.fdh.day05;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.fdh.day03.StockSourceFunction;
import org.fdh.pojo.StockPrice;

/**
 * 窗口算子-ReduceFunction和WindowProcessFunction结合，减少窗口缓存的数据量
 */
public class ReduceProcessWindowDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<StockPrice> stream = env.addSource(new StockSourceFunction("stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<StockPrice>forMonotonousTimestamps()
                        .withTimestampAssigner((input, timestamp) -> input.ts));

        //1对一映射
        stream.map(input -> Tuple4.of(input.symbol, input.price, input.price, input.ts))
                //解决泛型类型消除问题
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.LONG))
                .keyBy(s -> s.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .reduce(new MaxMinReduceFunction(), new WindowEndProcessFunction())
                .print();

        env.execute("ReduceProcessWindowDemo");
    }

    public static class MaxMinReduceFunction implements ReduceFunction<Tuple4<String, Double, Double, Long>> {
        @Override
        public Tuple4<String, Double, Double, Long> reduce(Tuple4<String, Double, Double, Long> value1, Tuple4<String, Double, Double, Long> value2) throws Exception {
            return Tuple4.of(value1.f0, Math.max(value1.f1, value2.f1), Math.min(value1.f1, value2.f1), value1.f3);
        }
    }

    public static class WindowEndProcessFunction extends ProcessWindowFunction<Tuple4<String, Double, Double, Long>, Tuple4<String, Double, Double, Long>, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Tuple4<String, Double, Double, Long>, Tuple4<String, Double, Double, Long>, String, TimeWindow>.Context context, Iterable<Tuple4<String, Double, Double, Long>> elements, Collector<Tuple4<String, Double, Double, Long>> out) throws Exception {

            if (elements.iterator().hasNext()) {
                Tuple4<String, Double, Double, Long> next = elements.iterator().next();
                //构建窗口结束时间
                out.collect(Tuple4.of(next.f0, next.f1, next.f2, context.window().getEnd()));
            }
        }
    }
}
