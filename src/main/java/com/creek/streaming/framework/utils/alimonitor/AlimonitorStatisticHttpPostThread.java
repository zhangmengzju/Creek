package com.creek.streaming.framework.utils.alimonitor;

import com.alibaba.jstorm.daemon.worker.metrics.AlimonitorClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 定时上报数据到alimonitor的线程
 * Created by zhuoling.lzl on 2015/3/11.
 */
public class AlimonitorStatisticHttpPostThread implements Runnable {
    private final static long IDLE = 30000L;                // 30秒
    private static AlimonitorClient alimonitorClient;

    static {
        alimonitorClient = new AlimonitorClient(AlimonitorClient.DEFAUT_ADDR,
                AlimonitorClient.DEFAULT_PORT, true);
        alimonitorClient.setMonitorName("jaeStormRunningData");     //监控项名称
    }

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

                Map<String, Object> sendMap = new HashMap<String, Object>();
                AtomicLong usedTime;
                for (Map.Entry entry : tmp.entrySet()) {
                    sendMap.put((String) entry.getKey(), entry.getValue());
                    usedTime = timeTmp.get(entry.getKey());
                    if (usedTime != null) {
                        long meanTime = usedTime.longValue() / ((AtomicInteger) entry.getValue()).longValue();
                        sendMap.put(entry.getKey() + "Time", meanTime);         //
                    }
                }
                //当前接口send(Map<String,Object> msg)上报格式有问题，只能用send(List<Map<String,Object>> msg).
                List<Map<String, Object>> msgList = new ArrayList<Map<String, Object>>();
                msgList.add(sendMap);

                //单次发送消息不能超过最大限制 （默认32MB）
                alimonitorClient.send(msgList);
            } catch (Throwable t) {
                AlimonitorStatisticUtils.logger.error("StatisticLogThread error.", t);
            }
        }
    }
}