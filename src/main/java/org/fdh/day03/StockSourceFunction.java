package org.fdh.day03;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.fdh.pojo.StockPrice;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StockSourceFunction implements SourceFunction<StockPrice> {

    /**
     * 保证多子任务(线程)互相的可见性（并行度问题）
     */
    private volatile boolean isRunning = true;

    private String path;

    private InputStream streamSource;

    public StockSourceFunction(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<StockPrice> ctx) throws Exception {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmmss");
        // 从项目的resources目录获取输入
        streamSource = this.getClass().getClassLoader().getResourceAsStream(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(streamSource));
        String line;
        boolean isFirstLine = true;
        long timeDiff = 0;
        long lastEventTs = 0;
        while (isRunning && (line = br.readLine()) != null) {
            String[] itemStrArr = line.split(",");
            LocalDateTime dateTime = LocalDateTime.parse(itemStrArr[1] + " " + itemStrArr[2], formatter);
            long eventTs = Timestamp.valueOf(dateTime).getTime();
            if (isFirstLine) {
                // 从第一行数据提取时间戳
                lastEventTs = eventTs;
                isFirstLine = false;
            }
            StockPrice stock = StockPrice.of(itemStrArr[0], Double.parseDouble(itemStrArr[3]), eventTs, Integer.parseInt(itemStrArr[4]));
            // 输入文件中的时间戳是从小到大排列的
            // 新读入的行如果比上一行大，sleep，这样来模拟一个有时间间隔的输入流
            timeDiff = eventTs - lastEventTs;
            if (timeDiff > 0)
                Thread.sleep(timeDiff);
            ctx.collect(stock);
            lastEventTs = eventTs;
        }


    }

    @Override
    public void cancel() {
        if (streamSource != null) {
            try {
                streamSource.close();
            } catch (IOException e) {
                System.out.println(e.toString());
            }
        }
        //总线上该变量失效，其他线程引用将从主内存获取
        isRunning = false;
    }
}
