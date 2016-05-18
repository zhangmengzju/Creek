package com.creek.streaming.framework.utils;

import com.creek.streaming.framework.producer.monitor.Bolt;

public class Utils {    
    public void insertQueue(long id) {
        if ((Bolt.queue.size() <= 10000) && (Bolt.queue.contains(id) == false)) {
            Bolt.queue.add(id);//SynchronizedSet中除iterator外的方法都是线程安全的
            synchronized (Bolt.queue) {
                Bolt.queue.notifyAll();
            }
        }
        System.out.println(String.format("[activeUser.size after add %d]", Bolt.queue.size()));
    }
    
    public static void hello(){
        System.out.println("Hello Groovy");
    }
}