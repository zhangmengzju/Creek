package com.creek.streaming.framework.utils.alimonitor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 定时写监控数据到日志，再由alimonitor收集
 * Created by zhuoling.lzl on 2015/3/11.
 */
public class AlimonitorStatisticLogThread implements Runnable {
    private final static long IDLE = 30000L;                // 30秒

    @SuppressWarnings("rawtypes")
    public void run() {
        while (true) {
            try {
                Thread.sleep(IDLE);
                Map<String, AtomicInteger> tmp;
                Map<String, AtomicLong> timeTmp;
                //拷贝statisticMap
                synchronized (AlimonitorStatisticUtils.statisticMapLock) {
                    tmp = AlimonitorStatisticUtils.statisticMap;
                    timeTmp = AlimonitorStatisticUtils.statisticTimeMap;
                    AlimonitorStatisticUtils.statisticMap = new HashMap<String, AtomicInteger>(16);
                    AlimonitorStatisticUtils.statisticTimeMap = new HashMap<String, AtomicLong>(16);
                }
                AtomicLong usedTime;
                for (Map.Entry entry : tmp.entrySet()) {
                    AlimonitorStatisticUtils.logger.info(System.currentTimeMillis() / 1000 + " " + entry.getKey() + " " + entry.getValue());
                    usedTime = timeTmp.get(entry.getKey());
                    if (usedTime != null) {
                        long meanTime = usedTime.longValue() / ((AtomicInteger) entry.getValue()).longValue();
                        AlimonitorStatisticUtils.logger.info(System.currentTimeMillis() / 1000 + " " + entry.getKey() + "Time " + meanTime);
                    }
                }
            } catch (Throwable t) {
                AlimonitorStatisticUtils.logger.error("StatisticLogThread error.", t);
            }
        }
    }
}