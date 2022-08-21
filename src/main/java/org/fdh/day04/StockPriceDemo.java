package org.fdh.day04;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.fdh.day03.StockSourceFunction;
import org.fdh.pojo.StockPrice;

public class StockPriceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<StockPrice> streamOperator = env.addSource(new StockSourceFunction("stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StockPrice>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.ts)
                );

        streamOperator.keyBy(in -> in.symbol)
                .process(new IncreaseAlertFunction(2000))
                .print();
        env.execute("timer process");
    }
}
