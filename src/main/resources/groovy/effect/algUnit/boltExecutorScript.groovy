package groovy.effect.algUnit
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Random;

import com.alibaba.dt.guider.streaming.framework.producer.effect.algUnit.AggregateMsgInMem
import com.alibaba.dt.guider.streaming.framework.producer.effect.algUnit.JoinMsgToMem
import com.alibaba.dt.guider.streaming.framework.producer.effect.algUnit.MemToMySQL
import com.alibaba.dt.guider.streaming.framework.producer.effect.algUnit.RecreateMsg
import com.alibaba.dt.guider.streaming.framework.producer.effect.algUnit.object.MemKey;
import com.alibaba.dt.guider.streaming.framework.producer.effect.algUnit.object.MemVal;
import com.alibaba.dt.guider.streaming.framework.producer.effect.algUnit.object.MsgObject;
import com.alibaba.dt.guider.streaming.framework.producer.effect.algUnit.BoltForEffect;
import com.alibaba.dt.guider.streaming.framework.producer.effect.algUnit.ManualTaskUseGroovyClassLoader;

import backtype.storm.tuple.Tuple;
import groovy.transform.CompileStatic

@CompileStatic
long manualChangeMsgKVs(Object[] args) {     
    if(args.length != 6){
        BoltForEffect.LOGGER.error("[BoltForEffect]"  
                       + "[args.length:" + args.length + "]"
                       + "[args:" + args + "]" );
        return -1L;
    }
    Tuple tuple = (Tuple)args[0]; 
    RecreateMsg rm = (RecreateMsg)args[1];
    JoinMsgToMem jmtm  = (JoinMsgToMem)args[2];
    MemToMySQL mtm = (MemToMySQL)args[3];
    AggregateMsgInMem amim = (AggregateMsgInMem)args[4];
    long lastUpdateMySQLTime = (Long)args[5];
        
        
    int size = tuple.size();
    ArrayList<MsgObject> msgObjList = null;
    if (size == 3) {//TT : AE App Msg
        BoltForEffect.LOGGER.debug("[AE Msg]" + tuple);
        MsgObject msgObj = rm.processMsg(tuple, "ae", "us")
        if(msgObj != null){
            msgObjList = new ArrayList<MsgObject>();
            msgObjList.add(msgObj);
        }
    } 
    else 
    if (size == 5) {//TT : Metis Detail Msg
        BoltForEffect.LOGGER.debug("[Metis Msg]" + tuple);        
        msgObjList = rm.sublimeMsgKVsNew(tuple, "metis", "us"); 
    } 
    else {
        BoltForEffect.LOGGER.warn("[Bolt][tuple size exception]" + tuple);
    }
    
    BoltForEffect.LOGGER.debug("[Bolt][msgObjList]" + msgObjList);
    ///利用HBase去join来自AE Msg和Metis Msg,然后数据上传到Memory中的HashMap<MemKey, MemVal>中
    //Msg -> HBase -> Mem
    jmtm.msgObjToHBaseToMemNew(msgObjList);

    //需要加random，使得不同bolt线程更新MySQL的操作均分在一分钟内
    long curTime = System.currentTimeMillis();
    long random1MinInMs = (Math.abs((new Random()).nextInt()) % 60) * 1000;
    if (curTime - lastUpdateMySQLTime > ManualTaskUseGroovyClassLoader.TIME_1_MIN_IN_MS + random1MinInMs) {
        ConcurrentHashMap<MemKey, MemVal> memKVs = amim.getMemKVs();
        BoltForEffect.LOGGER.error("[BoltForEffect]" + "[curTime:" + curTime + "]"
                + "[lastUpdateMySQLTime:" + lastUpdateMySQLTime + "]" + "[memKVs]"
                + memKVs.toString());
        for (Entry<MemKey, MemVal> ent : memKVs.entrySet()) {
            HashMap<String, Object> tmp = mtm.scatter(ent);
            tmp.put("scene_branch", "NULL");
            tmp.put("exec_unit_id", "NULL");
            tmp.put("gmt_create", "1970-01-01 00:00:00");
            tmp.put("gmt_modified", "1970-01-01 00:00:00");
            
            //1. alg_unit_id=xxx
            mtm.memKVToMySQL(tmp);

            //2. alg_unit_id=ALL
            tmp.put("alg_unit_id", "ALL");  
            mtm.memKVToMySQL(tmp);
        }
        amim.clearMemKVs();
        lastUpdateMySQLTime = curTime;
    }     
    return lastUpdateMySQLTime;
}

manualChangeMsgKVs args 

