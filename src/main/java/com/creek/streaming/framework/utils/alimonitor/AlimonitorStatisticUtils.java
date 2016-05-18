package com.creek.streaming.framework.utils.alimonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * alimonitor统计工具类
 * Created by zhuoling.lzl on 2015/3/11.
 */
public class AlimonitorStatisticUtils {
    public static final Logger logger = LoggerFactory.getLogger(AlimonitorStatisticUtils.class);
    public static final Object statisticMapLock = new Object();

    public static volatile Map<String, AtomicInteger> statisticMap = new HashMap<String, AtomicInteger>(16);
    public static volatile Map<String, AtomicLong> statisticTimeMap = new HashMap<String, AtomicLong>(16);
    private static Thread statisticLogThread;

    //初始化定时线程
    static {
        statisticLogThread = new Thread(new AlimonitorStatisticHttpPostThread(), "AlimonitorStatisticThread");
        statisticLogThread.setDaemon(true);      //true时，父线程退出时，子线程也退出。
        statisticLogThread.start();
    }

    /**
     * 简单计数，每次+1
     *
     * @param key
     */
    public static void count(String key) {
        count(key, 1);
    }

    /**
     * 简单计数，每次+1，并累加耗时usedTime.写日志时会计算平均值。
     *
     * @param key      监控项关键字，自由定义。耗时默认为key后面加‘Time’。
     * @param usedTime
     */
    public static void count(String key, Long usedTime) {
        count(key, 1, usedTime);
    }

    /**
     * 计数，增加value次。
     *
     * @param key
     * @param value
     */
    public static void count(String key, int value) {

        AtomicInteger i = statisticMap.get(key);
        if (i != null) {
            i.addAndGet(value);
        } else {
            synchronized (statisticMapLock) {
                if (statisticMap.get(key) == null) {
                    statisticMap.put(key, new AtomicInteger(value));
                }
            }
        }
    }

    /**
     * 计数，增加value次，并累加耗时usedTime。写日志时会计算平均值。
     *
     * @param key
     * @param value
     * @param usedTime
     */
    public static synchronized void count(String key, int value, Long usedTime) {
        AtomicInteger i = statisticMap.get(key);
        if (i != null) {
            i.addAndGet(value);
        } else {
            synchronized (statisticMapLock) {
                if (statisticMap.get(key) == null) {
                    statisticMap.put(key, new AtomicInteger(value));
                }
            }
        }
        AtomicLong j = statisticTimeMap.get(key);
        if (j != null) {
            j.addAndGet(usedTime);
        } else {
            synchronized (statisticMapLock) {
                if (statisticTimeMap.get(key) == null) {
                    statisticTimeMap.put(key, new AtomicLong(usedTime));
                }
            }
        }
    }
}
