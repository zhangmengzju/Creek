package groovy.effect.algUnit
import java.util.HashMap;
import com.alibaba.dt.guider.streaming.framework.producer.effect.algUnit.ManualTaskUseGroovyClassLoader;
import com.alibaba.dt.guider.streaming.framework.utils.DateUtils;
import com.alibaba.dt.guider.streaming.framework.utils.Utils;
import groovy.transform.CompileStatic
//time,scene_id,scene_branch,item_list,exec_unit_id
@CompileStatic
ArrayList<HashMap<String, Object>> manualChangeMsgKVs(Object[] args) {
    if(args.length != 2)
        return null;
    
    HashMap<String, Object> oriMsgKVs = (HashMap<String, Object>) args[0];
    String country = (String) args[1];
    
    ArrayList<HashMap<String, Object>> msgKVsList = new ArrayList<HashMap<String, Object>>();
    
    String tupleTime = (String) oriMsgKVs.get("time");
    long tupleTimeLong = DateUtils.getTimeLongByCountry("us", tupleTime);
    String tupleTimeIn15Min = DateUtils.getTimeIn15MinStringByCountry(country, tupleTimeLong);
    long tupleTimeIn15MinLong = DateUtils.getTimeLongByCountry(country, tupleTime);
    int tupleTSSegmentId = ((int)(tupleTimeIn15MinLong / ManualTaskUseGroovyClassLoader.TIME_15_MIN_IN_MS)) % ManualTaskUseGroovyClassLoader.RECORD_NUM_IN_48_HOURS;
     
    //tmpPairArr:{(itemId1 \003 algUnitId1), (itemId2 \003 algUnitId2)...}
    String[] tmpPairArr = ((String) oriMsgKVs.get("item_list")).split("\004");
        
    for(String tmpPair : tmpPairArr){
        String[]  tmpEleArr = tmpPair.split("\003")
        if(tmpEleArr.size() == 2){
            HashMap<String, Object> tmpMsgKVs = new HashMap<String, Object>();
            
            String itemId = tmpEleArr[0];
            String algUnitId = tmpEleArr[1];
            tmpMsgKVs.put("item_id", itemId);
            tmpMsgKVs.put("alg_unit_id", algUnitId);
            
            tmpMsgKVs.put("ts", tupleTimeIn15Min);
            tmpMsgKVs.put("ts_segment_id", tupleTSSegmentId);
            tmpMsgKVs.put("scene_id", oriMsgKVs.get("scene_id"));
            tmpMsgKVs.put("scene_branch", oriMsgKVs.get("scene_branch"));
            tmpMsgKVs.put("exec_unit_id", oriMsgKVs.get("exec_unit_id"));
            msgKVsList.add(tmpMsgKVs);
        }
    }
    return msgKVsList;
}

manualChangeMsgKVs args