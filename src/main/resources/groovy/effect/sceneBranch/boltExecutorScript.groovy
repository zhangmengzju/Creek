package groovy.effect.sceneBranch
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Random;
import com.alibaba.dt.guider.streaming.framework.producer.effect.sceneBranch.ManualTaskUseGroovyClassLoader;
import com.alibaba.dt.guider.streaming.framework.producer.effect.sceneBranch.MemToMySQL
import com.alibaba.dt.guider.streaming.framework.producer.effect.sceneBranch.RecreateMsg
import com.alibaba.dt.guider.streaming.framework.producer.effect.sceneBranch.JoinMsgToMem
import com.alibaba.dt.guider.streaming.framework.producer.effect.sceneBranch.AggregateMsgInMem
import com.alibaba.dt.guider.streaming.framework.producer.effect.sceneBranch.object.MemKey;
import com.alibaba.dt.guider.streaming.framework.producer.effect.sceneBranch.object.MemVal;
import com.alibaba.dt.guider.streaming.framework.producer.effect.sceneBranch.object.MsgObject;
import com.alibaba.dt.guider.streaming.framework.producer.effect.sceneBranch.BoltForEffect;

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
    MsgObject msgObj = null;
    if (size == 3) {//TT : AE App Msg
        BoltForEffect.LOGGER.debug("[AE Msg]" + tuple);
        msgObj = rm.processMsg(tuple, "ae", "us");
    } else if (size == 5) {//TT : Metis Detail Msg
        BoltForEffect.LOGGER.debug("[Metis Msg]" + tuple);
        msgObj = rm.processMsg(tuple, "metis", "us");
    } else {
        BoltForEffect.LOGGER.warn("[Bolt][tuple size exception]" + tuple);
    }

    ///利用HBase去join来自AE Msg和Metis Msg,然后数据上传到Memory中的HashMap<MemKey, MemVal>中
    //Msg -> HBase -> Mem
    jmtm.msgObjToHBaseToMem(msgObj);

    //需要加random，使得不同bolt线程更新MySQL的操作均分在一分钟内
    long curTime = System.currentTimeMillis();
    long random1MinInMs = (Math.abs((new Random()).nextInt()) % 60) * 1000;
    if (curTime - lastUpdateMySQLTime > ManualTaskUseGroovyClassLoader.TIME_1_MIN_IN_MS + random1MinInMs) {
        ConcurrentHashMap<MemKey, MemVal> memKVs = amim.getMemKVs();
        BoltForEffect.LOGGER.error("[BoltForEffect]" + "[curTime:" + curTime + "]"
                + "[lastUpdateMySQLTime:" + lastUpdateMySQLTime + "]" + "[memKVs]"
                + memKVs.toString());
        for (Entry<MemKey, MemVal> ent : memKVs.entrySet()) {
            //1. scene_branch=b1
            HashMap<String, Object> tmp = mtm.scatter(ent);
            mtm.memKVToMySQL(tmp);

            //2. scene_branch=ALL
            tmp.put("scene_branch", "ALL");
            mtm.memKVToMySQL(tmp);
        }
        amim.clearMemKVs();
        lastUpdateMySQLTime = curTime;
    }     
    return lastUpdateMySQLTime;
}

manualChangeMsgKVs  args

