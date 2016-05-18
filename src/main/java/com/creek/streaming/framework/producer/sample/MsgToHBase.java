package com.creek.streaming.framework.producer.sample;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

import com.creek.streaming.framework.producer.effect.algUnit.SchemaInfo;
import com.creek.streaming.framework.store.hbase.HTableInstance;
import com.creek.streaming.framework.store.hbase.HTablePoolManager;
import com.creek.streaming.framework.utils.DateUtils;

public class MsgToHBase implements Serializable {
    private static final long        serialVersionUID = 5791664783131227812L;
    private static final Logger      LOGGER           = LoggerFactory.getLogger(MsgToHBase.class);

    //////HBase 
    private static HTablePoolManager htpm             = null;
    private HTableInstance           hti              = null;

    ///Metis消息来了后，用(sceneId,time,requestId)构造rowKey，将metis消息中内容put进HBase
    //+--------------------------------------------------------------------------+ 
    //|                  Metis Msg => HBase                                      | 
    //| +-----------------------------------------+-------+----+-----+---------+ |  
    //| |                rowkey                   |  col1 |col2|col3 |  col4   | |
    //| +-----------------------------------------+-------+----+-----+---------+ |
    //| |sceneId+ (Long.MAX_VALUE-time)+ requestId|sceneId|time| rid |item_list| | 
    //| +-----------------------------------------+-------+----+-----+---------+ |  
    //+--------------------------------------------------------------------------+ 
    public void initForHBase(String hbaseConf) throws IOException {
        LOGGER.warn("[hbaseConf]" + hbaseConf);        
        Properties properties = new Properties();
        properties.load(MsgToHBase.class.getResourceAsStream(hbaseConf));
        
        htpm = HTablePoolManager.getInstance((String) properties.get("HBASE_CONF_FILE_PATH"));
        hti = new HTableInstance((HTable) htpm.getPool().getTable(
                (String) properties.get("HBASE_TABLE_NAME")));
        LOGGER.warn("[HTablePoolManager:" + htpm + "][HTableInstance:" + hti + "]");      
    }

    ///msgObj -> HBase RowKey
    public String getHBaseRowKey(HashMap<String, Object> msgKVs) throws ParseException {
        String rowkey = null;
        if (msgKVs.containsKey("scene_id") && msgKVs.containsKey("time")
                && msgKVs.containsKey("request_id")) {
            String sceneId = (String) msgKVs.get("scene_id");
            String timeStr = (String) msgKVs.get("time");
            Long timeLong = DateUtils.DateToLong("yyyy-MM-dd HH:mm:ss", timeStr,
                    "America/Los_Angeles");
            String requestId = (String) msgKVs.get("request_id");
            
            rowkey = sceneId + "_" + (Long.MAX_VALUE - timeLong) + "_" + requestId;
        }
        return rowkey;
    }

    public HashMap<String, Object> getOriMsgKVsUseProperties(Tuple tuple) {
        HashMap<Integer, Entry<String, String>> msgSchema = null;
        msgSchema = SchemaInfo.getMsgSchemaForMetis();         

        HashMap<String, Object> msgKVs = new HashMap<String, Object>();
        for (int i = 0; i < tuple.size(); i++) {
            Entry<String, String> fieldNameAndType = msgSchema.get(i);
            String fieldName = fieldNameAndType.getKey();
            String fieldType = fieldNameAndType.getValue();
            LOGGER.debug(String.format("[getOriMsgKVsUseProperties][fieldName:%s][fieldType:%s]",
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
    
    //将HBase的一行，转化成一个包含rowkey和各列的HashMap
    public static HashMap<String, String> resultToHashMap(Result res) {
        HashMap<String, String> map = new HashMap<String, String>();

        KeyValue[] kvArr = res.raw();
        if (kvArr.length >= 1) {
            String rowkey = new String(kvArr[0].getRow());
            map.put("rowkey", rowkey);

            for (int i = 0; i < kvArr.length; i++) {
                String colName = new String(kvArr[i].getQualifier());
                String colVal = new String(kvArr[i].getValue());
                map.put(colName, colVal);
            }
        }
        LOGGER.debug("[resultToHashMap][map]" + map);
        return map;
    }
    
    public ArrayList<HashMap<String, String>> get100RequestDetail(String prefix){
        LOGGER.debug("[get100RequestDetail][prefix:" + prefix + "]");
        if (prefix == null || prefix.equals("")){
            return null;
        }

        ArrayList<HashMap<String, String>> res = new ArrayList<HashMap<String, String>>();
        //scan from hbase
        Iterator<Result> rsIter = hti.scan(prefix).iterator();
        if (rsIter != null && rsIter.hasNext() != false) {
            for (int i = 1; i <= 100; i++) {
                if (rsIter.hasNext()) {
                    Result hbaseRes = rsIter.next();
                    LOGGER.debug("[get100RequestDetail][hbaseRes:" + hbaseRes + "]");
                    HashMap<String, String> tmp = resultToHashMap(hbaseRes);
                    LOGGER.debug("[get100RequestDetail][tmp:" + tmp + "]");
                    res.add(tmp);
                    LOGGER.debug("[get100RequestDetail][i:"+ i +"]"
                            + "[res.size:" + res.size() + "]");
                } else {
                    LOGGER.warn("[get100RequestDetail][rsIter.hasNext()==false][i:" + i + "]");
                    break;
                }
            }
        } else {
            LOGGER.warn("[GetRequestDetail][scan hbase but get nothing][prefix:" + prefix + "]");
        }
        LOGGER.warn("[get100RequestDetail][res.size:" + res.size() + "]");        
        return res;
    }
    
    public void tupleToHBase(Tuple tuple) throws ParseException {
        if (tuple == null)
            return;

        HashMap<String, Object> msgKVs = getOriMsgKVsUseProperties(tuple);
        String rowkey = getHBaseRowKey(msgKVs);

        //msgKVs: HashMap<String, Object> => HashMap<String, String> => HBase
        HashMap<String, String> msgKVsForHBase = new HashMap<String, String>();
        for (String key : msgKVs.keySet()) {
            Object valObj = msgKVs.get(key);
            String valStr = objectToString(valObj);
            msgKVsForHBase.put(key, valStr);
        }

        //put into hbase 
        LOGGER.error("[msgKVsForHBase]" + msgKVsForHBase);
        hti.put(rowkey, "cf", msgKVsForHBase);

        //scan from hbase
        //String prefix = msgKVsForHBase.get("scene_id");
        //ArrayList<HashMap<String, String>> res = get100RequestDetail(prefix);
        //LOGGER.error("[tupleToHBase][rowkey:" + rowkey + "][msgKVsForHBase:" + msgKVsForHBase + "][prefix:" + prefix + "][get100RequestDetail][res.size:" + res.size() + "]");
    }
}
