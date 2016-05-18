package com.creek.streaming.framework.producer.effect.sceneBranch;

import java.io.IOException;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creek.streaming.framework.producer.effect.sceneBranch.object.MsgObject;
import com.creek.streaming.framework.store.hbase.HTableInstance;
import com.creek.streaming.framework.store.hbase.HTablePoolManager;
import com.creek.streaming.framework.utils.MD5Utils;

public class JoinMsgToMem implements Serializable {
    private static final long        serialVersionUID      = 5791664783131227812L;
    private static final Logger      LOGGER                = LoggerFactory
                                                                   .getLogger(JoinMsgToMem.class);

    ///////当前Msg中要组成MemKey的那部分在HBase中的列名，用来把Msg中数据在HBase中写入和读出
    ///////当前Msg中要组成MemVal的那部分在HBase中的列名，用来把Msg中数据在HBase中写入和读出
    private HashSet<String>          curMemKeyPartFields   = new HashSet<String>();
    private String                   curMemValPartField    = null;

    ///////另一个Msg中要组成MemKey的那部分在HBase中的列名，用来把HBase中数据拿出，以和Msg中数据拼接成MemKey
    ///////另一个Msg中要组成MemVal的那部分在HBase中的列名，用来把HBase中数据拿出，以和Msg中数据拼接成MemVal
    private HashSet<String>          otherMemKeyPartFields = new HashSet<String>();
    private String                   otherMemValPartField  = null;

    //////HBase 
    private static HTablePoolManager htpm                  = null;
    private HTableInstance           hti                   = null;

    public void initForHBase(String hbaseConf) throws IOException {
        LOGGER.warn("[hbaseConf]" + hbaseConf);
        
        Properties properties = new Properties();
        properties.load(JoinMsgToMem.class.getResourceAsStream(hbaseConf));
       
        htpm = HTablePoolManager.getInstance((String) properties.get("HBASE_CONF_FILE_PATH"));
        hti = new HTableInstance((HTable) htpm.getPool().getTable((String) properties.get("HBASE_TABLE_NAME")));
        LOGGER.warn(String
                .format("[Bolt prepare][HBase init done][HTablePoolManager instance member:%s][HTable instance member:%s]",
                        htpm.toString(), hti.toString()));
    }

    ///msgObj -> HBase RowKey 注意用到了MD5
    public String getHBaseRowKey(MsgObject msgObj) throws NoSuchAlgorithmException {
        String hbaseJoinPartVal = msgObj.getHBaseJoinPart().get(SchemaInfo.getSchemaForHBaseJoin().getKey());
        return MD5Utils.md5(hbaseJoinPartVal).substring(0, 4) + hbaseJoinPartVal;
    }

    //检查另一个消息的MemKeyPart是否全部在HBase中: 若otherMemKeyPartFields本身没内容，则返回true
    public boolean checkOtherMemKeyPartInHBase(HashMap<String, byte[]> row, String msgTypePart) {
        if (otherMemKeyPartFields == null || otherMemKeyPartFields.size() == 0) {
            LOGGER.debug("[*checkOtherMemKeyInHBase*0]" + "[msgTypePart:" + msgTypePart + "]"
                    + "[otherMemKeyPartFields:" + otherMemKeyPartFields + "]"
                    + "[curMemKeyPartFields:" + curMemKeyPartFields + "]" 
                    + "[row:" + row + "]");
            return true;
        }
        if (row != null) {
            LOGGER.debug("[*checkOtherMemKeyInHBase*1]" + "[msgTypePart:" + msgTypePart + "]"
                    + "[otherMemKeyPartFields:" + otherMemKeyPartFields + "]"
                    + "[curMemKeyPartFields:" + curMemKeyPartFields + "]" 
                    + "[row:" + row + "]");
            for (String otherKey : otherMemKeyPartFields) {
                if (row.containsKey(otherKey) == false)
                    return false;
            }
            return true;
        }
        return false;
    }

    //检查当前消息的MemValPart是否在HBase中
    public boolean checkCurMemValPartInHBase(HashMap<String, byte[]> row) {
        if (row != null && curMemValPartField != null
                && row.containsKey(curMemValPartField) == true)
            return true;
        else
            return false;
    }

    //检查另一个消息的MemValPart是否在HBase中
    public boolean checkOtherMemValPartInHBase(HashMap<String, byte[]> row) {
        if (row != null && otherMemValPartField != null
                && row.containsKey(otherMemValPartField) == true)
            return true;
        else
            return false;
    }

    //如果当前消息的MemValPart不在HBase中时，在HBase中对当前消息的MemValPart进行初始化
    public void initCurMemValPartInHBase(String rowkey) {
        hti.put(rowkey, "cf", curMemValPartField, 0L);
    }

    //当另一个消息的MemKeyPart在HBase中时，取出另一个消息的MemKeyPart
    public HashMap<String, String> getOtherMemKeyPartInHBase(HashMap<String, byte[]> row) {
        if (row == null)
            return null;
        HashMap<String, String> otherMemKeyPartInHBase = new HashMap<String, String>();
        for (String otherKey : otherMemKeyPartFields) {
            otherMemKeyPartInHBase.put(otherKey, Bytes.toString(row.get(otherKey)));
        }
        return otherMemKeyPartInHBase;
    }

    //当另一个消息的MemValPart在HBase中时，取出另一个消息的MemValPart
    public HashMap<String, Long> getOtherMemValPartInHBase(HashMap<String, byte[]> row) {
        if (row == null)
            return null;
        HashMap<String, Long> otherMemValPartInHBase = new HashMap<String, Long>();
        if (row.containsKey(otherMemValPartField))
            otherMemValPartInHBase.put(otherMemValPartField,
                    Bytes.toLong(row.get(otherMemValPartField)));
        return otherMemValPartInHBase;
    }

    public HashMap<String, Long> getCurMemValPartInHBase(HashMap<String, byte[]> row) {
        if (row == null)
            return null;
        HashMap<String, Long> curMemValPartInHBase = new HashMap<String, Long>();
        if (row.containsKey(curMemValPartField))
            curMemValPartInHBase.put(curMemValPartField, Bytes.toLong(row.get(curMemValPartField)));
        return curMemValPartInHBase;
    }

    //将当前消息的MemKeyPart和另一个消息的MemKeyPart进行组合
    //将当前消息的MemValPart和另一个消息的MemValPart进行组合
    public <T> HashMap<String, T> mergeCurPartAndOtherPart(HashMap<String, T> curPart,
                                                           HashMap<String, T> otherPart) {
        if (curPart != null && otherPart != null) {
            curPart.putAll(otherPart);
            return curPart;
        } else if (otherPart != null) {
            return otherPart;
        } else if (curPart != null) {
            return curPart;
        } else
            return null;
    }

    //1、有两种消息：AE前台消息和Metis后台消息
    //2、任何一种消息来到后，先根据消息中的requestId作为HBase中rowkey，将消息中的curMemKeyPart和curMemValPart落到HBase中
    //3、当HBase中的MemKeyPart包含了：AEMemKeyPart和MetisMemKeyPart两部分后，
    //   将HBase中的AEMemKeyPart和MetisMemKeyPart两部分，加到内存中的AEMemKeyPart和MetisMemKeyPart两部分上，
    //   然后将HBase中的AEMemKeyPart和MetisMemKeyPart两部分数据清零
    public void msgObjToHBaseToMem(MsgObject msgObj) throws IOException, NoSuchAlgorithmException {
        if (msgObj == null)
            return;
        String rowkey = getHBaseRowKey(msgObj);
        HashMap<String, byte[]> row = hti.getRow(rowkey, "cf");  

        String msgTypePart = msgObj.getMsgTypePart();
        if (msgTypePart.equals("ae")) {
            otherMemKeyPartFields.clear();
            otherMemKeyPartFields.addAll(SchemaInfo.getMetisMemKeyPartFields());
            otherMemValPartField = SchemaInfo.getSchemaForMetisForMemVal().getKey();
            curMemKeyPartFields.clear();
            curMemKeyPartFields.addAll(SchemaInfo.getAeMemKeyPartFields());
            curMemValPartField =  SchemaInfo.getSchemaForAEForMemVal().getKey();
            LOGGER.debug("[ae][otherMemKeyPartFields]" + otherMemKeyPartFields);
            LOGGER.debug("[ae][otherMemValPartField]" + otherMemValPartField);
            LOGGER.debug("[ae][msgObj]" + msgObj);
            LOGGER.debug("[ae][curMemKeyPartFields]" + curMemKeyPartFields);
            LOGGER.debug("[ae][curMemValPartField]" + curMemValPartField);
        } else {
            otherMemKeyPartFields.clear();
            otherMemKeyPartFields.addAll(SchemaInfo.getAeMemKeyPartFields());
            otherMemValPartField =  SchemaInfo.getSchemaForAEForMemVal().getKey();
            curMemKeyPartFields.clear();
            curMemKeyPartFields.addAll(SchemaInfo.getMetisMemKeyPartFields());
            curMemValPartField = SchemaInfo.getSchemaForMetisForMemVal().getKey();
            LOGGER.debug("[metis][otherMemKeyPartFields]" + otherMemKeyPartFields);
            LOGGER.debug("[metis][otherMemValPartField]" + otherMemValPartField);
            LOGGER.debug("[metis][msgObj]" + msgObj);
            LOGGER.debug("[metis][curMemKeyPartFields]" + curMemKeyPartFields);
            LOGGER.debug("[metis][curMemValPartField]" + curMemValPartField);
        }
        //HBase中肯定没有curMemKeyPart和curMemValPart,
        //所以只去检查下HBase中有无otherMemKeyPart和otherMemValPart
        //同时由于otherMemKeyPart没有初始值的概念，所以只需要初始化otherMemValPart
        //若该requestId对应的row以前没有otherMemValPart这列,则先将HBase中otherMemValPart这列初始化为零

        createMemKeyAndMemVal(msgObj, rowkey, row);
    }

    public void msgObjToHBaseToMemNew(ArrayList<MsgObject> msgObjList) throws IOException, NoSuchAlgorithmException {
        if (msgObjList == null)
            return;
        for(MsgObject msgObj : msgObjList){
            msgObjToHBaseToMem(msgObj);
        }         
    }
    
    public HashMap<String, Long> mergeCurMemVal(HashMap<String, Long> curMemValInMsg,
                                                HashMap<String, Long> curMemValInHBase) {
        long valInMsg = 0L;
        if (curMemValInMsg != null && curMemValInMsg.containsKey(curMemValPartField)) {
            valInMsg = curMemValInMsg.get(curMemValPartField);
        }

        long valInHBase = 0L;
        if (curMemValInHBase != null && curMemValInHBase.containsKey(curMemValPartField)) {
            valInHBase = curMemValInHBase.get(curMemValPartField);
        }

        HashMap<String, Long> res = new HashMap<String, Long>();
        res.put(curMemValPartField, valInMsg + valInHBase);
        return res;
    }

    public void createMemKeyAndMemVal(MsgObject msgObj, String rowkey, HashMap<String, byte[]> row) {
        //---4部分均可能出现null或者size()==0的情况

        //1. OtherMemKeyPart
        //OtherMemKeyPart没有初始值的概念，当前Msg无法也不用构造出这部分，只能看从HBase中能取出什么
        HashMap<String, String> otherMemKeyPartInHBase = getOtherMemKeyPartInHBase(row);

        //2. OtherMemValPart
        HashMap<String, Long> otherMemValPartInHBase = getOtherMemValPartInHBase(row);

        //3. curMemKeyPart
        HashMap<String, String> curMemKeyPartInMsg = msgObj.getMemKeyPart();

        //4. curMemValPart
        HashMap<String, Long> curMemValPartInMsg = msgObj.getMemValPart();
        HashMap<String, Long> curMemValPartInHBase = getCurMemValPartInHBase(row);//new HashMap<String, Long>();
        HashMap<String, Long> newCurMemValPart = mergeCurMemVal(curMemValPartInMsg,
                curMemValPartInHBase);

        //hbase{cur, other} + msg{cur} -> mem{cur, other}
        if (checkOtherMemKeyPartInHBase(row, msgObj.getMsgTypePart())) {
            //memKeyPart    
            HashMap<String, String> memKeyPart = mergeCurPartAndOtherPart(curMemKeyPartInMsg,
                    otherMemKeyPartInHBase);

            //memValPart
            HashMap<String, Long> memValPart = mergeCurPartAndOtherPart(newCurMemValPart,
                    otherMemValPartInHBase);
            LOGGER.debug("[*upload*][" + msgObj.getMsgTypePart() + "Msg]" + "[curMemValPartInMsg:"
                    + curMemValPartInMsg + "]" + "[curMemValPartInHBase:" + curMemValPartInHBase
                    + "]" + "[memKeyPart:" + memKeyPart + "]" + "[memValPart:" + memValPart + "]"
                    + "[rowkey:" + rowkey + "]");

            AggregateMsgInMem.aggragateCurMemValPart(memKeyPart, memValPart);

            //把val部分清零，保留key的部分以继续用来join
            HashMap<String, Long> clearValPart = new HashMap<String, Long>();
            clearValPart.put(SchemaInfo.getSchemaForMetisForMemVal().getKey(), 0L);//"item_click_count"
            clearValPart.put( SchemaInfo.getSchemaForAEForMemVal().getKey(), 0L);//"item_exp_count_s"
            //---最后将curMemKey和curMemVal一起更新到HBase中
            hti.put(rowkey, "cf", curMemKeyPartInMsg, clearValPart);
        } else { //hbase{cur} + msg{cur} -> hbase{cur}
            //---最后将curMemKey和curMemVal一起更新到HBase中
            hti.put(rowkey, "cf", curMemKeyPartInMsg, newCurMemValPart);
        }
    }
    //+---------------------------------------------------------+---------------------------------------+
    //|                          HBase                          |      Memory                           | 
    //|               AE Msg                                    |                                       | 
    //|                  |                                      | +--------------+--------------------+ |
    //|      +-----------+--------------------+                 | |    MemKey    |      MemVal        | |
    //|      ↓           ↓                    ↓                 | +-----+--------+--------+-----------+ |
    //| +---------++--------+-----------++--------+-----------+ | |     |        | Delta  |   Delta   | |
    //| |requestId|| AE Key | Metis Key || AE Val | Metis Val |==>|AEKey|MetisKey|AEMemKey|MetisMemVal| |
    //| +---------++--------+-----------++--------+-----------+ | +-----+--------+--------+-----------+ |
    //|      ↑                    ↑                      ↑      |                     ↓          ↓      |
    //|      +--------------------+----------------------+      | +-----+--------+--------+-----------+ |
    //|                           |                             | |AEKey|MetisKey|AEMemVal|MetisMemVal| |
    //|                      Metis Msg                          | +-----+--------+--------+-----------+ |
    //+---------------------------------------------------------+---------------------------------------+ 
    ///AE App消息来了后，用requestId构造rowKey去HBase中查找:
    //1、利用HBase得到otherMemKeyInHBase,curMemValInHBase,otherMemValInHBase
    //2、将MemKey,MemVal写入到内存的HashMap中
}
