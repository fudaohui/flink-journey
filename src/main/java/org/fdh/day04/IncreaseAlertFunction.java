package org.fdh.day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.fdh.pojo.StockPrice;

import java.text.SimpleDateFormat;


/**
 * 练习ProcessFunction之KeyedProcessFunction里面的timer
 * 需求背景：
 * 。我们现在想看一只股票未来是否一直上涨，如果一直上涨，则发送一个提示。
 * 1、如果新数据比上次数据的价格更高且目前没有注册Timer，则注册一个未来的Timer。
 * 2、如果在到达Timer时间之前价格降低，则把刚才注册的Timer删除；
 * 3、如果在到达Timer时间之前价格没有降低，则Timer时间到达后触发onTimer()，发送一个提示。
 */
public class IncreaseAlertFunction extends KeyedProcessFunction<String, StockPrice, String> {

    //状态句柄
    private ValueState<Double> lastPrice;
    private ValueState<Long> currentTimer;

    public IncreaseAlertFunction(long intervalMis) {
        this.intervalMis = intervalMis;
    }

    public IncreaseAlertFunction() {
    }

    /**
     * timer 定时器时间间隔
     */
    private long intervalMis;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastPrice = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastPrice", Types.DOUBLE));
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Types.LONG));
    }

    @Override
    public void processElement(StockPrice stock, KeyedProcessFunction<String, StockPrice, String>.Context ctx, Collector<String> out) throws Exception {

        if (lastPrice == null || lastPrice.value() == null) {
            //第一次无值，不处理
        } else {
            long curTimerTimestamp;
            if (null == currentTimer.value()) {
                curTimerTimestamp = 0;
            } else {
                curTimerTimestamp = currentTimer.value();
            }
            if (stock.price < lastPrice.value()) {
                //新流入的股价较低，删除timer
                ctx.timerService().deleteEventTimeTimer(curTimerTimestamp);
            } else if (stock.price > lastPrice.value() && curTimerTimestamp == 0) {
                //注册
                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + intervalMis);
                currentTimer.update(ctx.timestamp() + intervalMis);
            }


        }
        lastPrice.update(stock.price);
    }

    @Override
    public void onTimer(long ts, KeyedProcessFunction<String, StockPrice, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        out.collect(formatter.format(ts) + ", symbol: " + ctx.getCurrentKey() + " monotonically increased for " + intervalMis + " millisecond.");         // 清空currentTimer状态         currentTimer.clear();     }
    }
}
