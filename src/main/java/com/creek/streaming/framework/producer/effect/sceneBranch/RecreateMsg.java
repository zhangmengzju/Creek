package com.creek.streaming.framework.producer.effect.sceneBranch;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.codehaus.groovy.control.CompilationFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

import com.creek.streaming.framework.producer.effect.sceneBranch.object.MsgObject;

public class RecreateMsg implements Serializable {
    private static final long                              serialVersionUID        = 5672896779432730833L;
    private static final Logger                            LOGGER                  = LoggerFactory
                                                                                           .getLogger(RecreateMsg.class);
    private static String                                  country                 = null;        

    ////////////////////////Groovy////////////////////////////////////////////////////////
    public boolean filterForMsg(HashMap<String, Object> tmpMsgKVs, String msgTypePart) throws CompilationFailedException, InstantiationException, IllegalAccessException, IOException {
        String curFilterScriptName = null;
        if (msgTypePart.equals("metis")) {
            curFilterScriptName = "filterMetisMsgScript";
        } else {
            curFilterScriptName = "filterAEMsgScript";
        }
        return ManualTaskUseGroovyClassLoader.filterMsgKVs(curFilterScriptName, tmpMsgKVs);
    }

    public HashMap<String, Object> enrichMsgKVs(HashMap<String, Object> tmpMsgKVs, String msgTypePart, String country) throws IOException, CompilationFailedException, InstantiationException, IllegalAccessException {
        String curEnrichScriptName = null;
        if (msgTypePart.equals("metis")) {
            curEnrichScriptName = "enrichMetisMsgScript";
        } else {
            curEnrichScriptName = "enrichAEMsgScript" ;
        }
        return ManualTaskUseGroovyClassLoader.enrichMsgKVs(curEnrichScriptName, tmpMsgKVs, country);
    }
    
    public ArrayList<HashMap<String, Object>> enrichMsgKVsNew(HashMap<String, Object> tmpMsgKVs, String msgTypePart, String country) throws CompilationFailedException, InstantiationException, IllegalAccessException, IOException {
        String curEnrichScriptName = null;
        if (msgTypePart.equals("metis")) {
            curEnrichScriptName = "enrichMetisMsgScript";
        } else {
            curEnrichScriptName = "enrichAEMsgScript";
        }
        ArrayList<HashMap<String, Object>> msgKVsList = ManualTaskUseGroovyClassLoader.enrichMsgKVsNew(curEnrichScriptName, tmpMsgKVs, country);
        LOGGER.debug("[enrichMsgKVsNew][msgKVsList]" + msgKVsList);
        return msgKVsList;
    }

    public long getMemVal(HashMap<String, Object> tmpMsgKVs, String msgTypePart) throws CompilationFailedException, InstantiationException, IllegalAccessException, IOException {
        //metis后台的一个tuple对应的是一组item
        //AE前台的一个tuple对应的是一个item    
        return ManualTaskUseGroovyClassLoader.getMemVal("getMemValScript", tmpMsgKVs, msgTypePart);
    }
    ///////////////////////Groovy////////////////////////////////////////////////////////

    public HashMap<String, String> getMemKeyPart(HashMap<String, Object> tmpMsgKVs, String msgTypePart) {
        HashMap<String, String> memKeyPart = new HashMap<String, String>();

        HashMap<Integer, Entry<String, String>> curSchemaForMemKey = null;
        if (msgTypePart.equals("metis"))
            curSchemaForMemKey = SchemaInfo.getSchemaForMetisForMemKey();
        else
            curSchemaForMemKey = SchemaInfo.getSchemaForAEForMemKey();

        if (curSchemaForMemKey != null && curSchemaForMemKey.size() > 0) {
            for (int i = 0; i < curSchemaForMemKey.size(); i++) {
                String fieldName = curSchemaForMemKey.get(i).getKey();
                memKeyPart.put(fieldName, objectToString(tmpMsgKVs.get(fieldName)));
            }
        }
        return memKeyPart;
    }

    public HashMap<String, Long> getMemValPart(HashMap<String, Object> tmpMsgKVs, String msgTypePart) throws CompilationFailedException, InstantiationException, IllegalAccessException, IOException {
        HashMap<String, Long> memValPart = new HashMap<String, Long>();

        Entry<String, String> curSchemaForMemVal = null;
        if (msgTypePart.equals("metis"))
            curSchemaForMemVal = SchemaInfo.getSchemaForMetisForMemVal();
        else
            curSchemaForMemVal = SchemaInfo.getSchemaForAEForMemVal();

        memValPart.put(curSchemaForMemVal.getKey(), getMemVal(tmpMsgKVs, msgTypePart));
        return memValPart;
    }

    public MsgObject processMsg(Tuple tuple, String msgTypePart, String msgCountry) throws CompilationFailedException, InstantiationException, IllegalAccessException, IOException {
        /*
         * String logTime = tuple.getString(0);//2015-10-23 07:09:31 String
         * pageType = tuple.getString(1);//DETAIL String args =
         * tuple.getString(2
         * );//referPageId=008f391f-f97d-390d-9c45-dd4e8d14702b,
         * pageFrom=native,pageId
         * =c0fcf57d-897c-347c-99f6-8be1c127b6a5,_rid=20151023070859376
         * -1425277688
         * ,productId=2036073430,spm-cnt=a1z65.Detail.0.0,isbk=1,spm-url
         * =a1z65.Home.0.0,dep=727 String metisTime =
         * tuple.getString(0);//2015-10-22 09:00:01 String sceneId =
         * tuple.getString(1);//10143 String requestId =
         * tuple.getString(2);//20151022090001654-1846552184 String sceneBranch
         * = tuple.getString(3);//b3_sol String itemList =
         * tuple.getString(4);//"32460743137"
         * +"\003"+"sceneHotSol"+"\004"+"1760675706"+"\003"+"sceneHotSol" String
         * String execUnitId = tuple.getString(5);
         */
        country = msgCountry;
        HashMap<String, Object> msgKVs = getOriMsgKVsUseProperties(msgTypePart, tuple);
        if (filterForMsg(msgKVs, msgTypePart)) {
            try {
                msgKVs = enrichMsgKVs(msgKVs, msgTypePart, country);
            } catch (Exception e) {
                LOGGER.error("[processMsg][" + e + "][msgKVs:" + msgKVs + "]");
            }
            printData(msgKVs);
            /// MsgObject共4部分：
            /// | MsgTypePart | HBaseJoinPart | MemKeyPart1 | MemValPart1 | 
            
            ///[HBaseJoinPart]
            HashMap<String, String> hbaseJoinPart = new HashMap<String, String>();
            hbaseJoinPart.put(SchemaInfo.getSchemaForHBaseJoin().getKey(),
                    objectToString(msgKVs.get(SchemaInfo.getSchemaForHBaseJoin().getKey())));
            
            ///[MemKeyPart1] : msg中要组成key的那些部分，作为内存中与MySQL关联的HashMap的key的一部分
            /// {(ts=2015-06-09 19:15:00), (tsSegmentId=102), (sceneId=50001), (sceneBranch=b1)}
            HashMap<String, String> memKeyPart = getMemKeyPart(msgKVs, msgTypePart);
            
            ///[MemValPart1] : msg中要组成val的那些部分，作为内存中与MySQL关联的HashMap的val的一部分
            HashMap<String, Long> memValPart = getMemValPart(msgKVs, msgTypePart);

            return new MsgObject(msgTypePart, hbaseJoinPart, memKeyPart, memValPart);
        }
        return null;
    }

    public ArrayList<MsgObject> sublimeMsgKVsNew(Tuple tuple, String msgTypePart, String msgCountry) throws CompilationFailedException, InstantiationException, IllegalAccessException, IOException {
        /*=======AE Msg=====================================================
         * String logTime = tuple.getString(0);//2015-10-23 07:09:31 
         * String pageType = tuple.getString(1);//DETAIL 
         * 
         * String args = tuple.getString(2);//
         * referPageId=008f391f-f97d-390d-9c45-dd4e8d14702b,
         * pageFrom=native,pageId=c0fcf57d-897c-347c-99f6-8be1c127b6a5,
         * _rid=20151023070859376-1425277688,productId=2036073430,
         * spm-cnt=a1z65.Detail.0.0,isbk=1,spm-url=a1z65.Home.0.0,dep=727 
         * ======Metis Msg===================================================
         * String metisTime = tuple.getString(0);//2015-10-22 09:00:01 
         * String sceneId = tuple.getString(1);//10143 
         * String requestId = tuple.getString(2);//20151022090001654-1846552184 
         * String sceneBranch = tuple.getString(3);//b3_sol 
         * 
         * String itemList = tuple.getString(4);//
         * "32460743137" +"\003"+"sceneHotSol"+"\004"+
         * "1760675706"+"\003"+"sceneHotSol"
         * 
         * String execUnitId = tuple.getString(5);
         */
        ArrayList<MsgObject> msgObjList = new ArrayList<MsgObject>();
        
        country = msgCountry;
        HashMap<String, Object> msgKVs = getOriMsgKVsUseProperties(msgTypePart, tuple);
        if (filterForMsg(msgKVs, msgTypePart)) {
            ///time => 转化成15min粒度 => ts + tsSegmentId
            ArrayList<HashMap<String, Object>> msgKVsList = enrichMsgKVsNew(msgKVs, msgTypePart, country);
            printDataNew(msgKVsList);
            
            for(HashMap<String, Object> tmpMsgKVs : msgKVsList){
                /// MsgObject共4部分：
                /// | MsgTypePart | HBaseJoinPart | MemKeyPart1 | MemValPart1 | 
                
                ///[HBaseJoinPart]
                HashMap<String, String> hbaseJoinPart = new HashMap<String, String>();
                hbaseJoinPart.put(SchemaInfo.getSchemaForHBaseJoin().getKey(),
                        objectToString(tmpMsgKVs.get(SchemaInfo.getSchemaForHBaseJoin().getKey())));
                
                ///[MemKeyPart1] : msg中要组成key的那些部分，作为内存中与MySQL关联的HashMap的key的一部分
                /// {(ts=2015-06-09 19:15:00), (tsSegmentId=102), (sceneId=50001), (sceneBranch=b1)}
                HashMap<String, String> memKeyPart = getMemKeyPart(tmpMsgKVs, msgTypePart);
                            
                ///[MemValPart1] : msg中要组成val的那些部分，作为内存中与MySQL关联的HashMap的val的一部分
                HashMap<String, Long> memValPart = getMemValPart(tmpMsgKVs, msgTypePart);
                
                MsgObject tmpMsgObj = new MsgObject(msgTypePart, hbaseJoinPart, memKeyPart, memValPart);
                LOGGER.debug("[sublimeMsgKVsNew][tmpMsgObj:" + tmpMsgObj + "]");
                msgObjList.add(tmpMsgObj);
            }
            return msgObjList;
        }
        return null;
    }

    
    public HashMap<String, Object> getOriMsgKVsUseProperties(String msgTypePart, Tuple tuple) {
        HashMap<Integer, Entry<String, String>> msgSchema = null;
        if (msgTypePart.equals("metis"))
            msgSchema = SchemaInfo.getMsgSchemaForMetis();
        else
            msgSchema = SchemaInfo.getMsgSchemaForAE();

        HashMap<String, Object> msgKVs = new HashMap<String, Object>();

        for (int i = 0; i < tuple.size(); i++) {
            Entry<String, String> fieldNameAndType = msgSchema.get(i);
            String fieldName = fieldNameAndType.getKey();
            String fieldType = fieldNameAndType.getValue();
            LOGGER.debug(String.format("[getMsgKVsUseProperties][fieldName:%s][fieldType:%s]",
                    fieldName, fieldType));

            Object fieldValue = null;
            if (fieldType.equals("string")) {
                fieldValue = tuple.getString(i);
            } else if (fieldType.equals("int")) {
                fieldValue = Integer.valueOf(tuple.getString(i));
            } else if (fieldType.equals("long")) {
                fieldValue = Long.valueOf(tuple.getString(i));
            } else if (fieldType.equals("float")) {
                fieldValue = Float.valueOf(tuple.getString(i));
            } else if (fieldType.equals("double")) {
                fieldValue = Double.valueOf(tuple.getString(i));
            } else if (fieldType.equals("boolean")) {
                fieldValue = Boolean.valueOf(tuple.getString(i));
            }
            msgKVs.put(fieldName, fieldValue);
        }
        return msgKVs;
    }
    
    public static String objectToString(Object obj) {
        if (obj == null)
            return null;
        if (obj instanceof String) {
            return (String) obj;
        } else if (obj instanceof Integer) {
            return ((Integer) obj).toString();
        } else if (obj instanceof Long) {
            return ((Long) obj).toString();
        } else if (obj instanceof Float) {
            return ((Float) obj).toString();
        } else if (obj instanceof Double) {
            return ((Double) obj).toString();
        }
        return null;
    }

    public static void printData(HashMap<String, Object> msgKVs) {
        if (msgKVs != null) {
            for (String key : msgKVs.keySet()) {
                LOGGER.debug("[tuple][" + key + ":" + objectToString(msgKVs.get(key)) + "]");
            }
        }
    }
    
    public static void printDataNew(List<HashMap<String, Object>> msgKVsList) {
        if (msgKVsList != null) {
            LOGGER.debug("[tuple][" + msgKVsList + "]");
        }
    }
}
