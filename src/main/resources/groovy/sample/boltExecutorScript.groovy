package groovy.sample
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Random;

import com.alibaba.dt.guider.streaming.framework.producer.sample.MsgToHBase
import com.alibaba.dt.guider.streaming.framework.producer.sample.BoltForSample;
import backtype.storm.tuple.Tuple;
import groovy.transform.CompileStatic

@CompileStatic
void manualChangeMsgKVs(Object[] args) {     
    if(args.length != 2){
        BoltForSample.LOGGER.error("[BoltForSample]"  
                       + "[args.length:" + args.length + "]"
                       + "[args:" + args + "]" );
        return;
    }
    Tuple tuple = (Tuple)args[0]; 
    MsgToHBase mth  = (MsgToHBase)args[1];         
        
    int size = tuple.size();
    if (size == 14) {//TT : Metis Detail Msg
        BoltForSample.LOGGER.debug("[Metis Msg]" + tuple);        
        mth.tupleToHBase(tuple);  
    } 
    else {
        BoltForSample.LOGGER.warn("[Bolt][tuple size exception]" + tuple);
    }
}

manualChangeMsgKVs args 