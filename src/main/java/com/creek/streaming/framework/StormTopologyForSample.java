package com.creek.streaming.framework;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alibaba.da.plough.spout.MetaQPushSpout;
import com.alibaba.da.plough.spout.PloughSpoutFactory;
import com.alibaba.da.plough.spout.TimeTunnelConfig;
import com.alibaba.da.plough.spout.TimeTunnelSpout; 
import com.creek.streaming.framework.producer.sample.BoltForSample;
import com.creek.streaming.framework.source.SpoutInstance;
import com.creek.streaming.framework.utils.StreamFileUtils;

public class StormTopologyForSample {
    //////Log
    private static final Logger           LOGGER   = LoggerFactory.getLogger(StormTopologyForSample.class);

    ////create instances from conf files
    static String                         country  = null;
    static HashMap<String, SpoutInstance> spoutMap = new HashMap<String, SpoutInstance>();
    static Properties                     topoProp = new Properties();

    public static TopologyBuilder initTopology(String topoConfPath) throws IOException {
        try {
            LOGGER.warn("[topoConfPath][" + topoConfPath + "]");
            topoProp.load(StormTopologyForSample.class.getResourceAsStream(topoConfPath));
        } catch (IOException e) {
            throw new RuntimeException("topology properties file load error.", e);//用RuntimeException去中断程序运行
        }
        for (Entry<Object, Object> ent : topoProp.entrySet()) {
            String key = (String) ent.getKey();
            String val = (String) ent.getValue();
            LOGGER.warn(String.format("[topology][key:%s][val:%s]", key, val));
        }

        //////////////////////////////////////////////
        ////load spout properties
        int spoutInstanceNum = Integer.valueOf((String) topoProp.get("spout.instance.num"));
        for (int i = 1; i <= spoutInstanceNum; i++) {
            String type = (String) topoProp.get("spout.instance." + i + ".type");
            String name = (String) topoProp.get("spout.instance." + i + ".name");
            int parallelism = Integer.valueOf((String) topoProp.get("spout.instance." + i
                    + ".parallelism"));
            LOGGER.warn(String.format("[topology][type:%s][name:%s][parallelism:%s]", type, name,
                    parallelism));
            if (type.equals("tt")) {
                String conf = topoProp.getProperty("spout.instance." + i + ".conf");
                String split = topoProp.getProperty("spout.instance." + i + ".split");
                String groupingType = topoProp
                        .getProperty("spout.instance." + i + ".grouping.type");
                String groupingField = topoProp.getProperty("spout.instance." + i
                        + ".grouping.field");

                //TT -> Spout
                File tempFile = StreamFileUtils.stream2file(StormTopologyForSample.class
                        .getResourceAsStream(conf));
                TimeTunnelConfig ttc = TimeTunnelConfig.loadConfigFromFile(tempFile.getPath());

                if (split.equals("backslash005"))
                    split = "\005";
                else if (split.equals("backslash001"))
                    split = "\001";
                else if (split.equals("backslash004"))
                    split = "\004";
                ttc.setFieldSplit(split);
                TimeTunnelSpout tts = PloughSpoutFactory.createTimeTunnelSpout(ttc);

                SpoutInstance spIns = new SpoutInstance(tts, name, parallelism, groupingType,
                        groupingField);
                spoutMap.put(name, spIns);
            } else if (type.equals("metaq")) {
                String topic = topoProp.getProperty("spout.instance." + i + ".topic");
                String group = topoProp.getProperty("spout.instance." + i + ".group");
                String fields = topoProp.getProperty("spout.instance." + i + ".fields");
                String groupingType = topoProp
                        .getProperty("spout.instance." + i + ".grouping.type");
                String groupingField = topoProp.getProperty("spout.instance." + i
                        + ".grouping.field");

                //MetaQ -> Spout
                MetaQPushSpout mqps = new MetaQPushSpout(topic, group, fields);

                SpoutInstance spIns = new SpoutInstance(mqps, name, parallelism, groupingType,
                        groupingField);
                spoutMap.put(name, spIns);
            }
        }

        ///////////////////////////////////////////////
        //Topology setSpout
        TopologyBuilder builder = new TopologyBuilder();
        for (String name : spoutMap.keySet()) {
            SpoutInstance spoutIns = spoutMap.get(name);
            Object spout = spoutIns.getSpout();
            int num = spoutIns.getSpoutNum();
            if (spout instanceof TimeTunnelSpout) {
                builder.setSpout(name, (TimeTunnelSpout) spout, num);
            } else if (spout instanceof MetaQPushSpout) {
                builder.setSpout(name, (MetaQPushSpout) spout, num);
            }
        }

        ///////////////////////////////////////////////
        ////load bolt properties
        int boltNum = Integer.valueOf(topoProp.getProperty("bolt.num"));
        ///////////////////////////////////////////////
        //Topology setBolt 
        BoltDeclarer bd = builder.setBolt("processor", new BoltForSample(), boltNum);

        ///////////////////////////////////////////////
        //Topology setBolt grouping
        for (String spoutName : spoutMap.keySet()) {
            SpoutInstance spIns = spoutMap.get(spoutName);
            if (spIns.getGroupingType().equals("fieldsGrouping")) {
                bd.fieldsGrouping(spoutName, new Fields(spIns.getGroupingField()));
            } else if (spIns.getGroupingType().equals("shuffleGrouping")) {
                bd.shuffleGrouping(spoutName);
            }
        } 
         
        return builder;
    }

    public static void submitTopology(TopologyBuilder builder, String boltConfPath) throws AlreadyAliveException,
            InvalidTopologyException {
        int workerNum = Integer.valueOf(topoProp.getProperty("topology.worker.num"));
        int ackerNum = Integer.valueOf(topoProp.getProperty("topology.acker.num"));
        int maxSpoutPending = Integer.valueOf(topoProp.getProperty("topology.max.spout.pending"));
        ///////////////////////////////////////////////
        //Config
        Config conf = new Config();
        conf.setNumWorkers(workerNum);
        conf.setNumAckers(ackerNum);
        conf.setMaxSpoutPending(maxSpoutPending);//队列长度
        conf.setMessageTimeoutSecs(60);
        conf.setDebug(false);
        //JStorm 安装完后，默认的NIMBUS端口配置为7672
        conf.put(Config.NIMBUS_THRIFT_PORT, 7672);
        
        conf.put("boltConfPath", boltConfPath);

        // StormSubmitter.submitTopology("guider.quark.monitor.realtime."topoProp.getProperty("topology.name"),
        // conf, builder.createTopology());
        StormSubmitter.submitTopology("guider.quark.prehost.sample." + country,
                conf, builder.createTopology());
        LOGGER.warn("storm cluster will start");
    }

    public static void main(String[] args) throws IOException, AlreadyAliveException,
            InvalidTopologyException {          
        if (args.length < 3) {
            LOGGER.error(String.format("args num error!"));
            for (int i = 0; i < args.length; i++) {
                LOGGER.error(String.format("[args %d ] %s", i, args[i]));
            }
            return;
        } 
        country = String.valueOf(args[0]);//"us"
        String topoConfPath = String.valueOf(args[1]);//"/properties/effect/topology.properties"
        String boltConfPath = String.valueOf(args[2]);//"/properties/effect/bolt.properties"
        
        TopologyBuilder builder = initTopology(topoConfPath);
        submitTopology(builder, boltConfPath);
    }
}