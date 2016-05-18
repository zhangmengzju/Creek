package com.creek.streaming.framework.producer.effect.sceneBranch;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creek.streaming.framework.producer.effect.sceneBranch.object.MemKey;
import com.creek.streaming.framework.producer.effect.sceneBranch.object.MemVal;
import com.creek.streaming.framework.utils.PreparedStatementUtils;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.group.jdbc.TGroupDataSource;

public class MemToMySQL implements Serializable {
    private static final long       serialVersionUID         = -7977975899237510332L;
    private static final Logger     LOGGER                   = LoggerFactory
                                                                     .getLogger(MemToMySQL.class);

    //////TDDL
    private static String           tableName                = null;
    private static TGroupDataSource tgds                     = null;

    //////MySQL Fields For Clean
    private HashMap<String, String> setFieldsForClean        = null;
    private ArrayList<String>       equalFieldsForClean      = null;
    private ArrayList<String>       notEqualFieldsForClean   = null;

    //////MySQL Fields For Update
    private ArrayList<String>       setFieldsForUpdate       = null;
    private ArrayList<String>       sumFieldsForUpdate       = null;
    private ArrayList<String>       maxFieldsForUpdate       = null;

    //////MySQL Fields For Manual Change
    private HashMap<String, Object> setFieldsForMaunalChange = null;

    public void initForMySQL(String mysqlConf) throws IOException {
        //load mysql config
        Properties properties = new Properties();
        properties.load(MemToMySQL.class.getResourceAsStream(mysqlConf));        

        ////For TDDL
        if (tableName == null) {
            tableName = properties.getProperty("tddl.table.name");
        }
        if (tgds == null) {
            tgds = new TGroupDataSource();
            tgds.setAppName(properties.getProperty("tddl.app.name"));
            tgds.setDbGroupKey(properties.getProperty("tddl.db.group.key"));
            try {
                tgds.init();
            } catch (TddlException e) {
                LOGGER.error(String.format("[Bolt][TDDL init error] %s", e));
                throw new RuntimeException(String.format("[Bolt][TDDL init error] %s", e));
            }
        }

        ////For Clean
        setFieldsForClean = new HashMap<String, String>();
        equalFieldsForClean = new ArrayList<String>();
        notEqualFieldsForClean = new ArrayList<String>();
        ////For Update
        setFieldsForUpdate = new ArrayList<String>();
        sumFieldsForUpdate = new ArrayList<String>();
        maxFieldsForUpdate = new ArrayList<String>();
        ////For Manual Change 
        setFieldsForMaunalChange = new HashMap<String, Object>();

        for (Entry<Object, Object> ent : properties.entrySet()) {
            String key = (String) ent.getKey();
            String val = (String) ent.getValue();
            if (key.startsWith("clean.set.")) {
                setFieldsForClean.put(key.replaceFirst("clean.set.", ""), val);
            } else if (key.equals("clean.condition.equal")) {
                String[] vals = val.split(",");
                for (String str : vals)
                    equalFieldsForClean.add(str);
            } else if (key.equals("clean.condition.not.equal")) {
                String[] vals = val.split(",");
                for (String str : vals)
                    notEqualFieldsForClean.add(str);
            } else if (key.equals("update.set")) {
                String[] vals = val.split(",");
                for (String str : vals)
                    setFieldsForUpdate.add(str);
            } else if (key.equals("update.sum")) {
                String[] vals = val.split(",");
                for (String str : vals)
                    sumFieldsForUpdate.add(str);
            } else if (key.equals("update.max")) {
                String[] vals = val.split(",");
                for (String str : vals)
                    maxFieldsForUpdate.add(str);
            } else if (key.startsWith("manual.change.")) {
                setFieldsForMaunalChange.put(key.replaceFirst("manual.change.", ""), val);
            }
        }
    }

    public void monitorMySQL(ArrayList<Object> valsForClean,//For Clean
                             HashMap<String, String> setFields, ArrayList<String> equalFields,
                             ArrayList<String> notEqualFields,
                             ArrayList<Object> valsForUpdate,//For Update
                             ArrayList<String> fieldsForSet, ArrayList<String> fieldsForSum,
                             ArrayList<String> fieldsForMax) throws SQLException {
        long t1 = System.currentTimeMillis();
        Connection conn = tgds.getConnection();

        long t2 = System.currentTimeMillis();
        monitorCleanMySQL(conn, valsForClean, setFields, equalFields, notEqualFields);

        long t3 = System.currentTimeMillis();
        monitorUpdateMySQL(conn, valsForUpdate, fieldsForSet, fieldsForSum, fieldsForMax);

        long t4 = System.currentTimeMillis();
        LOGGER.debug(String.format("[*cost*][mysql][conn %s ms][clean %s ms][update %s ms]",
                    String.valueOf(t2 - t1), String.valueOf(t3 - t2), String.valueOf(t4 - t3)));

        if (conn != null)
            conn.close();     
    }

    ///1.1 实时流量监控monitor：清理48h前的数据
    public void monitorCleanMySQL(Connection conn, ArrayList<Object> valsForClean,
                                  HashMap<String, String> setFields, ArrayList<String> equalFields,
                                  ArrayList<String> notEqualFields) throws SQLException {
        //把相同segmentId的，但不是最近15min内而是48h前的消息清空
        //UPDATE guider_quark_monitor_rt_cn 
        //SET ts=?,
        //max_concurrent=0,exec_count=0,recovery_count=0,
        //zero_result_count=0,response_time=0,oceanus_queue_time=0,oceanus_cpu_cost
        //WHERE ts_segment_id=? AND scene_id=? AND scene_branch=? AND ts<>?" 
        
        PreparedStatement psClean = PreparedStatementUtils.getCleanPreparedStatement(conn,
                    tableName, setFields, equalFields, notEqualFields);
        LOGGER.debug(String.format("[monitorCleanMySQL][valsForClean.size:%d]",
                    valsForClean.size()));
        for (int i = 0; i < valsForClean.size(); i++) {
            Object val = valsForClean.get(i);
            int psLoc = i + 1;//PreparedStatement的下标从1开始，而非从零开始
            if (val instanceof Integer) {
                psClean.setInt(psLoc, (Integer) val);
            } else if (val instanceof Long) {
                psClean.setLong(psLoc, (Long) val);
            } else if (val instanceof String) {
                psClean.setString(psLoc, (String) val);
            } else if (val instanceof Double) {
                psClean.setDouble(psLoc, (Double) val);
            } else if (val instanceof Float) {
                psClean.setFloat(psLoc, (Float) val);
            } else if (val instanceof Boolean) {
                psClean.setBoolean(psLoc, (Boolean) val);
            }
        }

        int res = psClean.executeUpdate();
        LOGGER.debug(String.format("[res = psClean.executeUpdate()]%d", res));

        if (psClean != null)
            psClean.close();
    }

    ///1.2 实时流量监控monitor：用Msg的数据更新MySQL
    public void monitorUpdateMySQL(Connection conn, 
                                   ArrayList<Object> valsForUpdate,
                                   ArrayList<String> fieldsForSet, 
                                   ArrayList<String> fieldsForSum,
                                   ArrayList<String> fieldsForMax) throws SQLException {
        //INSERT INTO guider_quark_monitor_rt_us
        //(ts_segment_id, ts, scene_id, scene_branch,
        //exec_count, recovery_count, zero_result_count,
        //response_time, oceanus_queue_time, oceanus_cpu_cost,
        //max_concurrent)
        //VALUES (?,?,?,?,?,?,?,?,?,?,?)
        //ON DUPLICATE KEY UPDATE
        //exec_count = exec_count + ?,
        //recovery_count = recovery_count + ?,
        //zero_result_count = zero_result_count + ?,
        //response_time = response_time + ?,
        //oceanus_queue_time = oceanus_queue_time + ?,
        //oceanus_cpu_cost = oceanus_cpu_cost + ?,
        //max_concurrent = GREATEST(max_concurrent, ?) 
        //这里的Unique Key由ts_segment_id,scene_id,scene_branch组成
        PreparedStatement psReplace = PreparedStatementUtils.getReplacePreparedStatement(conn,
                    tableName, fieldsForSet, fieldsForSum, fieldsForMax);
        LOGGER.debug(String.format("[monitorUpdateMySQL][valsForUpdate.size:%d]",
                    valsForUpdate.size()));
        for (int i = 0; i < valsForUpdate.size(); i++) {
            Object val = valsForUpdate.get(i);
            int psLoc = i + 1;//PreparedStatement的下标从1开始，而非从零开始
            if (val instanceof Integer) {
                psReplace.setInt(psLoc, (Integer) val);
            } else if (val instanceof Long) {
                psReplace.setLong(psLoc, (Long) val);
            } else if (val instanceof String) {
                psReplace.setString(psLoc, (String) val);
            } else if (val instanceof Double) {
                psReplace.setDouble(psLoc, (Double) val);
            } else if (val instanceof Float) {
                psReplace.setFloat(psLoc, (Float) val);
            } else if (val instanceof Boolean) {
                psReplace.setBoolean(psLoc, (Boolean) val);
            }
        }

        int res = psReplace.executeUpdate();
        LOGGER.debug(String.format("[res = psReplace.executeUpdate()]%d", res));

        if (psReplace != null)
            psReplace.close();        
    }

    public HashMap<String, Object> scatter(Entry<MemKey, MemVal> memKV) {
        HashMap<String, Object> tmp = new HashMap<String, Object>();

        if (memKV != null) {
            MemKey memKey = memKV.getKey();
            MemVal memVal = memKV.getValue();

            if (memKey != null) {
                for (String memKeyField : SchemaInfo.getMemKeyFields()) {
                    tmp.put(memKeyField, memKey.getValue(memKeyField));
                }
            }
            if (memVal != null) {
                for (String memValField : SchemaInfo.getMemValFields()) {
                    tmp.put(memValField, memVal.getValue(memValField));
                }
            }
        }
        return tmp;
    }

    public void memKVToMySQL(HashMap<String, Object> tmp) throws SQLException {
        /////////////////////////////////////////////////////////       
        //1. 过滤得到MySQL需要的Vals
        ArrayList<Object> msgValsForClean = msgKVsMatchMySQLFieldsForClean(tmp, setFieldsForClean,
                equalFieldsForClean, notEqualFieldsForClean);
        ArrayList<Object> msgValsForUpdate = msgKVsMatchMySQLFieldsForUpdate(tmp,
                setFieldsForUpdate, sumFieldsForUpdate, maxFieldsForUpdate);

        LOGGER.debug(String.format("[msgToMySQL]"
                + "[tmp.size:%d][msgValsForClean.size:%d][msgValsForUpdate.size:%d]", tmp.size(),
                msgValsForClean.size(), msgValsForUpdate.size()));

        /////////////////////////////////////////////////////////
        //2. 将消息中的Vals按规则更新到MySQL的fields中       
        monitorMySQL(msgValsForClean, setFieldsForClean, equalFieldsForClean,
                notEqualFieldsForClean, msgValsForUpdate, setFieldsForUpdate, sumFieldsForUpdate,
                maxFieldsForUpdate);
    }

    //////////////////////////////////////////////////////////////////////////
    ////For Clean
    //过滤msg中的kv, 只保留msg key 与 mysql field 一致的那些msg kv
    public ArrayList<Object> msgKVsMatchMySQLFieldsForClean(HashMap<String, Object> msgKVs,
                                                            HashMap<String, String> fieldsForSet,
                                                            ArrayList<String> fieldsForEqual,
                                                            ArrayList<String> fieldsForNotEqual) {
        LOGGER.debug(String.format("[Clean][msgKVs.size:%d][fieldsForSet.size:%d]"
                + "[fieldsForEqual.size:%d][fieldsForNotEqual.size:%d]", msgKVs.size(), 
                fieldsForSet.size(), fieldsForEqual.size(), fieldsForNotEqual.size()));

        ArrayList<Object> msgVals = new ArrayList<Object>();
        for (String field : fieldsForSet.keySet()) {
            if (msgKVs.containsKey(field)) {
                if (fieldsForSet.get(field).equals("?")) {
                    msgVals.add(msgKVs.get(field));
                }
            }
        }
        for (String field : fieldsForEqual) {
            if (msgKVs.containsKey(field)) {
                msgVals.add(msgKVs.get(field));
            }
        }
        for (String field : fieldsForNotEqual) {
            if (msgKVs.containsKey(field)) {
                msgVals.add(msgKVs.get(field));
            }
        }

        return msgVals;
    }

    //////////////////////////////////////////////////////////////////////////    
    ////For Update
    //过滤msg中的kv, 只保留msg key 与 mysql field 一致的那些msg kv
    public ArrayList<Object> msgKVsMatchMySQLFieldsForUpdate(HashMap<String, Object> msgKVs,
                                                             ArrayList<String> fieldsForSet,
                                                             ArrayList<String> fieldsForSum,
                                                             ArrayList<String> fieldsForMax) {
        LOGGER.debug(String.format("[Update][msgKVs.size:%d][fieldsForSet.size:%d]"
                + "[fieldsForSum.size:%d][fieldsForMax.size:%d]", msgKVs.size(),
                fieldsForSet.size(), fieldsForSum.size(), fieldsForMax.size()));

        ArrayList<Object> msgVals = new ArrayList<Object>();
        for (String field : fieldsForSet) {
            if (msgKVs.containsKey(field)) {
                msgVals.add(msgKVs.get(field));
            }
        }
        for (String field : fieldsForSum) {
            if (msgKVs.containsKey(field)) {
                msgVals.add(msgKVs.get(field));
            }
        }
        for (String field : fieldsForMax) {
            if (msgKVs.containsKey(field)) {
                msgVals.add(msgKVs.get(field));
            }
        }
        return msgVals;
    }
}
