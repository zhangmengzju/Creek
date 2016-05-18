package com.creek.streaming.framework.producer.sample;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creek.streaming.framework.producer.effect.algUnit.ManualTaskUseGroovyClassLoader;
import com.creek.streaming.framework.producer.effect.algUnit.SchemaInfo;
import com.creek.streaming.framework.producer.effect.algUnit.ScriptInfo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class BoltForSample implements IRichBolt {
    private static final long  serialVersionUID = -6144987053284416998L;
    public static final Logger LOGGER           = LoggerFactory.getLogger(BoltForSample.class);
    private OutputCollector    _collector;

    private MsgToHBase         mth              = new MsgToHBase();

    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
                        OutputCollector collector) {
        try {            
            Properties boltProp = new Properties();
            String boltConfPath = (String)conf.get("boltConfPath");
            boltProp.load(BoltForSample.class.getResourceAsStream(boltConfPath));        
            for (Entry<Object, Object> ent : boltProp.entrySet()) {
                String key = (String) ent.getKey();
                if (key.equals("scriptInfo")) {
                    //1. msg script
                    //"/properties/sample/scriptInfo.properties"
                    ScriptInfo.initForScript(boltProp.getProperty(key));
                } else if (key.startsWith("msg.schema.")) {
                    //2. msg schema
                    //"/properties/sample/msg-schema-metis.properties"
                    SchemaInfo.initForSchema(boltProp.getProperty(key));
                } else if (key.equals("hbase.schema")) {
                    //4. hbase        
                    //"/properties/sample/hbase-schema.properties"
                    mth.initForHBase(boltProp.getProperty(key));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("load properties file load error.", e);//用RuntimeException去中断程序运行
        }
        
        //storm
        _collector = collector;
    }

    public void execute(Tuple tuple) {
        if (tuple != null) {
            LOGGER.debug(String.format("[Bolt][tuple][%d]%s", tuple.size(), tuple.toString()));
        }
        
        long t1 = System.currentTimeMillis();
        try {
            ManualTaskUseGroovyClassLoader.boltExecute("boltExecutorScript", 
                    new Object[] { tuple, mth });
        } catch (Throwable e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String fullInfo = sw.toString();

            if (tuple != null)
                LOGGER.error(String.format("[Throwable Error][tuple:%s][error:%s]",
                        tuple.toString(), fullInfo));
            else
                LOGGER.error("[Throwable Error][tuple is null][error:%s]", fullInfo);
            _collector.ack(tuple);
            return;
        }
        long t2 = System.currentTimeMillis();
        LOGGER.debug(String.format("[*cost*][bolt][%s ms]", String.valueOf(t2 - t1)));

        _collector.ack(tuple);
    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
