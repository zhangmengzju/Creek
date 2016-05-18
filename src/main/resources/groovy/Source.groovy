package groovy
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alibaba.da.plough.spout.MetaQPushSpout;
import com.alibaba.da.plough.spout.PloughSpoutFactory;
import com.alibaba.da.plough.spout.TimeTunnelConfig;
import com.alibaba.da.plough.spout.TimeTunnelSpout;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.dt.guider.streaming.framework.producer.monitor.Bolt;

//////Log
def LOGGER = LoggerFactory.getLogger(StormTopology.class);

def init(){
//        int workerNum, int ackerNum, int ttPCSpoutNum, int ttAPPSpoutNum,
//                              int metaqDelaySpoutNum, int boltNum, int maxSpoutPending,
//                              int consumerThreadNum, String country) 
    //////////////////////////////////////////////////////
    //project config
    def properties = new Properties();
    properties.load(getClass().getClassLoader().getResourceAsStream("/" + country.toLowerCase()
                + "-guider-storm.properties"));
    

    //////////////////////////////////////////
    //TT: PC端
    //{country}-tt-api-rule-pc.properties
    //读TT配置文件:inputStream to tempFilePC
    def tempFilePC = StreamFileUtils.stream2file(getClass().getClassLoader().getResourceAsStream("/"
                + country.toLowerCase() + "-tt-api-rule-pc.properties"));
   
    def ttApiRuleConfigPC = TimeTunnelConfig.loadConfigFromFile(tempFilePC
            .getPath());
    ttApiRuleConfigPC.setFieldSplit("\005");
//
//    //////////////////////////////////////////
//    //TT: APP端
//    //{country}-tt-api-rule-app.properties
//    //读TT配置文件:inputStream to tempFileAPP
    def tempFileAPP = StreamFileUtils.stream2file(getClass().getClassLoader().getResourceAsStream("/"
                + country.toLowerCase() + "-tt-api-rule-app.properties"));
    def ttApiRuleConfigAPP = TimeTunnelConfig.loadConfigFromFile(tempFileAPP
            .getPath());
    ttApiRuleConfigAPP.setFieldSplit("\001");

    ///////////////////////////////////////////////
    //TimeTunnel -> Spout_1
    def ttsPC = PloughSpoutFactory.createTimeTunnelSpout(ttApiRuleConfigPC);

    ///////////////////////////////////////////////
    //TimeTunnel -> Spout_2
    def ttsAPP = PloughSpoutFactory.createTimeTunnelSpout(ttApiRuleConfigAPP);

    ///////////////////////////////////////////////
    //MetaQ -> Spout_3
    //延时的MetaQ数据，收到后对应memberSeq_categoryId的count减一
    def mqDelay = new MetaQPushSpout("ae_beacon_pageview_delay_30min",
            "guider_metaq_group", "member_id,product_id,gmt_log,reduce_one");

    ///////////////////////////////////////////////
//    //Topology
//    def builder = new TopologyBuilder();
//    builder.setSpout("tt_spout_pc", ttsPC, ttPCSpoutNum);
//    builder.setSpout("tt_spout_app", ttsAPP, ttAPPSpoutNum);
//    builder.setSpout("metaq_delay_spout", mqDelay, metaqDelaySpoutNum);
//
//    builder.setBolt("processor", new Bolt(consumerThreadNum, country), boltNum)
//            .fieldsGrouping("tt_spout_pc", new Fields("member_id"))
//            .fieldsGrouping("tt_spout_app", new Fields("long_login_nick"))//确认是member_id
//            .fieldsGrouping("metaq_delay_spout", new Fields("member_id"));
//
//    ///////////////////////////////////////////////
//    //Config
//    def conf = new Config();
//    conf.setNumWorkers(workerNum);
//    conf.setMaxSpoutPending(maxSpoutPending);//队列长度
//    conf.setMessageTimeoutSecs(60);
//    conf.setNumAckers(ackerNum);
//    conf.setDebug(false);
//    conf.put(Config.NIMBUS_THRIFT_PORT, 7672);
//    ConfigExtension.setUserDefinedLog4jConf(conf, "udf.log4j.properties");
//    StormSubmitter.submitTopology("guider_storm", conf, builder.createTopology());
    LOGGER.warn("storm cluster will start");
            
}
        
init        