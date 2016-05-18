package com.creek.streaming.framework.producer.effect.sceneBranch;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creek.streaming.framework.producer.effect.sceneBranch.object.MemKey;
import com.creek.streaming.framework.producer.effect.sceneBranch.object.MemVal;

public class AggregateMsgInMem implements Serializable {
    private static final long                        serialVersionUID = -5599898967475324745L;
    private static final Logger                      LOGGER           = LoggerFactory
                                                                              .getLogger(AggregateMsgInMem.class);

    //+-------------------------+---------------------------------------------------+
    //|         MemKey          |                      MemVal                       |
    //+-------------------------+-------------------------+-------------------------+
    //| "ts" = "xxx 12:15:00"   |      AEMemVal           |      MetisMemVal        |
    //| "ts_segment_id" = "102" +-------------------------+-------------------------+
    //| "scene_id" = "50012"    | "item_click_count"      | "item_exp_count_s"      |
    //| "scene_branch" = "b1"   |   = (HBase + Msg) + Mem |   = (HBase + Msg) + Mem |
    //+-------------------------+-------------------------+-------------------------+   

    //key：MemKey(metisTimeEvery5Min, tsSegmentId, sceneId, sceneBranch)//, execUnitId, algUnitId)
    //value: MemVal(item_exp_count_s, item_click)
    //////Memory中保存的最近一分钟的数据，由多个地方对其补充修改   
    private static ConcurrentHashMap<MemKey, MemVal> memKVs           = new ConcurrentHashMap<MemKey, MemVal>();

    //检查能否组合出有值的MemKey对象
    public static boolean checkMemKeyPart(HashMap<String, String> memKeyPart) {
        if (SchemaInfo.getMemKeyFields() == null || SchemaInfo.getMemKeyFields().size() == 0
                || memKeyPart == null || memKeyPart.size() == 0) {
            return false;
        } else {
            for (String memKey : SchemaInfo.getMemKeyFields()) {//memKey的field全存在才返回true
                if (memKeyPart.get(memKey) == null)
                    return false;
            }
            return true;
        }
    }

    //检查能否组合出有值的MemVal对象
    public static boolean checkMemValPart(HashMap<String, Long> memValPart) {
        if (SchemaInfo.getMemValFields() == null || SchemaInfo.getMemValFields().size() == 0
                || memValPart == null || memValPart.size() == 0) {
            return false;
        } else {
            for (String memVal : SchemaInfo.getMemValFields()) {//memVal有一个field存在就返回true
                if (memValPart.get(memVal) != null)
                    return true;
            }
            return false;
        }
    }

    //(HBase + Msg) => MemKey
    public static MemKey getMemKeyObjFromMap(HashMap<String, String> memKeyPart) {
        if (checkMemKeyPart(memKeyPart)) {
            HashMap<String, String> memKeyMap = new HashMap<String, String>();
            for (String memKeyField : SchemaInfo.getMemKeyFields()) {
                memKeyMap.put(memKeyField, memKeyPart.get(memKeyField));
            }
            return new MemKey(memKeyMap);
        }
        return null;
    }

    //(HBase + Msg) => MemVal
    public static MemVal getMemValObjFromMap(HashMap<String, Long> memValPart) {
        if (checkMemValPart(memValPart)) {
            HashMap<String, Long> memValMap = new HashMap<String, Long>();
            for (String memValField : SchemaInfo.getMemValFields()) {
                long memVal = (memValPart.get(memValField) != null) ? memValPart.get(memValField)
                        : 0L;
                memValMap.put(memValField, memVal);
            }
            return new MemVal(memValMap);
        }
        return null;
    }

    //MemVal1 + MemVal2 => newMemVal
    public static MemVal mergeMemVal(MemVal mv1, MemVal mv2) {
        if (mv1 != null && mv2 != null) {
            for (String memValField : SchemaInfo.getMemValFields()) {
                long v1 = mv1.getValue(memValField) >= 0 ? mv1.getValue(memValField) : 0;
                long v2 = mv2.getValue(memValField) >= 0 ? mv2.getValue(memValField) : 0;
                mv1.setValue(memValField, v1 + v2);
            }
            return mv1;
        } else if (mv1 == null) {
            return mv2;
        } else if (mv2 == null) {
            return mv1;
        } else
            return null;
    }

    public static void aggragateCurMemValPart(HashMap<String, String> memKeyPart,
                                              HashMap<String, Long> memValPart) {
        MemKey memKey = getMemKeyObjFromMap(memKeyPart);
        if (memKey != null) {
            //MemValFromHBase
            MemVal memValFromHBase = getMemValObjFromMap(memValPart);

            //MemValFromMem
            MemVal memValFromMem = memKVs.get(memKey);

            //MemVal = MemValFromMem + MemValFromHBase
            MemVal memVal = mergeMemVal(memValFromMem, memValFromHBase);
            memKVs.put(memKey, memVal);

            LOGGER.debug("[AggregateMsgInMem][memKVs]" + memKVs);
        }
    }

    public ConcurrentHashMap<MemKey, MemVal> getMemKVs() {
        return memKVs;
    }

    public void clearMemKVs() {
        memKVs.clear();
    }

    public static void main(String[] args) {
        HashMap<String, Long> m = new HashMap<String, Long>();
        m.put("xx", 99L);

        System.out.println("[s1]" + m.get("xx"));
        //System.out.println("[s2]" + m.get("yy"));
        //long yy = m.get("yy");
        //System.out.println("[s3]" + yy);

        System.out.println("[Long.max]" + Long.MAX_VALUE);
        String str = "9223372036854775807";
        System.out.println(str.getBytes().length);
    }
}
