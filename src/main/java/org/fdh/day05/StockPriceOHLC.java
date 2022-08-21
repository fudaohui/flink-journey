package org.fdh.day05;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.fdh.day03.StockSourceFunction;
import org.fdh.pojo.StockPrice;

/**
 * 1min的K线数据
 */
public class StockPriceOHLC {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //新版本不需要额外设置时间属性
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置所有算子的并行度为1
        env.setParallelism(1);
        SingleOutputStreamOperator<StockPrice> inputStream = env.addSource(new StockSourceFunction("stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <StockPrice>forMonotonousTimestamps()//设置水位线生成策略，时间是持续递增的
                        .withTimestampAssigner((input, timestamp) -> input.ts));//从ts取字段作为时间戳

        inputStream.keyBy(input -> input.symbol)
//                .timeWindow(Time.minutes(1))
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new OHLCProcessFunction())
                .print();

        env.execute("1-minutes stock price");

    }
}
