package org.fdh.day05;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.fdh.pojo.StockPrice;

/**
 * 也是二合一的操作，返回的类型可以不一样，比ReduceFunction功能更丰富
 * Tuple3,窗口内股票价格总和sum和出现次数count
 */
public class AverageAggregateFunction implements AggregateFunction<StockPrice, Tuple3<String, Double, Integer>, Tuple2<Double, Integer>> {
    //在一次新的Aggregate Function发起时，创建一个新的Accumulator，Accumulator中的值是我们所说的中间状态数据，即ACC数据
    @Override
    public Tuple3<String, Double, Integer> createAccumulator() {
        return Tuple3.of("", 0.0, 0);
    }

    //当一个新元素流入时，将新元素与ACC数据合并，返回ACC数据
    @Override
    public Tuple3<String, Double, Integer> add(StockPrice value, Tuple3<String, Double, Integer> accumulator) {
        return Tuple3.of(value.symbol, value.price + accumulator.f1, accumulator.f2 + 1);
    }

    //将中间数据转换成结果数据
    @Override
    public Tuple2<Double, Integer> getResult(Tuple3<String, Double, Integer> accumulator) {
        //平均数，出现的次数
        return Tuple2.of(accumulator.f1 / accumulator.f2, accumulator.f2);
    }

    //将两个ACC数据合并
    //note:会话窗口模式下，窗口长短是不断变化的，多个窗口有可能合并为一个窗口，多个窗口内的ACC会合并为一个。窗口融合时，Flink会调用merge()方法，将多个ACC合并在一起，生成新的ACC。
    @Override
    public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
        System.out.println("AverageAggregateFunction merge has work");
        return null;
    }
}
