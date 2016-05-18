package com.creek.streaming.framework.producer.effect.algUnit;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class BoltForEffect implements IRichBolt {
    private static final long   serialVersionUID    = -6144987053284416998L;
    public  static final Logger LOGGER              = LoggerFactory.getLogger(BoltForEffect.class);
    private OutputCollector     _collector;

    private long                lastUpdateMySQLTime = 0L;
    private RecreateMsg         rm                  = new RecreateMsg();
    private JoinMsgToMem        jmtm                = new JoinMsgToMem();
    private MemToMySQL          mtm                 = new MemToMySQL();
    private AggregateMsgInMem   amim                = new AggregateMsgInMem();

    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
                        OutputCollector collector) {
        try {
            Properties boltProp = new Properties();
            String boltConfPath = (String)conf.get("boltConfPath");      
            boltProp.load(BoltForEffect.class.getResourceAsStream(boltConfPath));
            for (Entry<Object, Object> ent : boltProp.entrySet()) {
                String key = (String) ent.getKey();
                if(key.equals("scriptInfo")){
                    //1. msg script
                    //"/properties/effect/scriptInfo.properties"
                    ScriptInfo.initForScript(boltProp.getProperty(key));
                } else if(key.startsWith("msg.schema.")){
                    //2. msg schema
                    //"/properties/effect/msg-schema-ae.properties"
                    //"/properties/effect/msg-schema-metis.properties"
                    SchemaInfo.initForSchema(boltProp.getProperty(key));
                } else if(key.equals("mysql.schema")){
                    //3. mysql        
                    //"/properties/effect/mysql-schema.properties"
                    mtm.initForMySQL(boltProp.getProperty(key));
                } else if(key.equals("hbase.schema")){
                    //4. hbase        
                    //"/properties/effect/hbase-schema.properties"
                    jmtm.initForHBase(boltProp.getProperty(key));
                }
            }   
        } catch (IOException e) {
            throw new RuntimeException("boltConf properties file load error.", e);//用RuntimeException去中断程序运行
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
            Object[] args = {tuple, rm, jmtm, mtm, amim, lastUpdateMySQLTime};  
            lastUpdateMySQLTime = ManualTaskUseGroovyClassLoader.boltExecute(
                    "boltExecutorScript", args);                 
        } catch (Throwable e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String fullInfo = sw.toString();

            if (tuple != null)
                LOGGER.error(String.format("[Catch Throwable Error][tuple:%s][error:%s]",
                        tuple.toString(), fullInfo));
            else
                LOGGER.error("[Catch Throwable Error][tuple is null][error:%s]", fullInfo);
            _collector.ack(tuple);
            return;
        }

        long t2 = System.currentTimeMillis();
        LOGGER.debug(String.format("[*cost*][bolt][%s ms]",
                String.valueOf(t2 - t1)));

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
