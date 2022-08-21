package org.fdh.day04;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.fdh.pojo.StockPrice;

/**
 * 自定义触发器，注意会覆盖默认的触发器
 * 需求：我们比较关注价格急跌的情况，默认窗口Size是60秒，如果价格跌幅超过5%，则立即执行窗口处理函数，
 * 如果价格跌幅在1%~5%，那么10秒后触发窗口处理函数
 */
public class MyTriggerFunction extends Trigger<StockPrice, TimeWindow> {
    @Override
    public TriggerResult onElement(StockPrice element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

    }
}
