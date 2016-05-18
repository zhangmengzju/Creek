package com.creek.streaming.framework.producer.monitor;

import java.util.LinkedHashSet;
import java.util.Map;

import com.creek.streaming.framework.consumer.ConsumerTask;
import com.creek.streaming.framework.store.Store;
import com.creek.streaming.framework.target.Target;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class Bolt implements IRichBolt {

    private static final long serialVersionUID = 8089541085316705156L;

    //////Queue
    public static LinkedHashSet<Long> queue = new LinkedHashSet<Long>();
    
    //////Storm
    private OutputCollector          _collector;

    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
                        OutputCollector collector) {
        ///Store Part
        Store.init();
        
        ///Target Part
        Target.init();
        
        ///Consumer Part
        Thread t = new Thread(new ConsumerTask(queue));
        t.start();  
        
        //////JStorm
        _collector = collector;
    }
   
    /////////////////////////////////////////////////////////////////////////////////////////////
    //JStorm
    public void execute(Tuple tuple) {
      
        ///Producer Part
        ProducerTask.run();

        _collector.ack(tuple);
    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Map getComponentConfiguration() {
        return null;
    }
}
