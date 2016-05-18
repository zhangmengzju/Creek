package com.creek.streaming.framework;

import com.creek.streaming.framework.consumer.ConsumerTask;
import com.creek.streaming.framework.producer.monitor.Bolt;
import com.creek.streaming.framework.producer.monitor.ProducerTask;

public class Test {

    public static void main(String[] args) {
      
        ///Producer Part
        ProducerTask.run();
        
        ///Consumer Part
        Thread t = new Thread(new ConsumerTask(Bolt.queue));
        t.start(); 
    }

}
