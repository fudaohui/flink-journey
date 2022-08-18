package org.fdh.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.fdh.pojo.StockPrice;

public class StockPriceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1、获取实时股票的最大值-keyby ,max
        DataStreamSource<StockPrice> dataStreamSource = env.addSource(new StockSourceFunction("stock/stock-tick-20200108.csv"));
//        dataStreamSource.keyBy(in->in.symbol)
//                .max("price")
//                .print();
//
        //2、汇率转换-one input and transform to one output
//        dataStreamSource.map(in -> StockPrice.of(in.symbol, in.price * 7, in.ts, in.volume))
//                .print();

        //

        dataStreamSource.filter(in -> in.price > 300).print();
        env.execute(" stock price");
    }
}
