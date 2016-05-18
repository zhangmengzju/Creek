package com.creek.streaming.framework.producer.monitor;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.creek.streaming.framework.utils.PreparedStatementUtils;
import com.creek.streaming.framework.utils.StreamFileUtils;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.group.jdbc.TGroupDataSource;

public class BoltForMonitor implements IRichBolt {
    private static final long                       serialVersionUID         = 1L;

    //////Log
    private static final Logger                     LOGGER                   = LoggerFactory
                                                                                     .getLogger(BoltForMonitor.class);

    //////TDDL
    private TGroupDataSource                        tgds                     = null;
    private String                                  tableName                = null;                                   ;
  
    //////MySQL Fields For Clean
    private HashMap<String, String>                 setFieldsForClean        = null;
    private ArrayList<String>                       equalFieldsForClean      = null;
    private ArrayList<String>                       notEqualFieldsForClean   = null;

    //////MySQL Fields For Update
    private ArrayList<String>                       setFieldsForUpdate       = null;
    private ArrayList<String>                       sumFieldsForUpdate       = null;
    private ArrayList<String>                       maxFieldsForUpdate       = null;

    //////MySQL Fields For Manual Change
    private HashMap<String, Object>                 setFieldsForMaunalChange = null;

    //////Msg Fields And Types
    private HashMap<Integer, Entry<String, String>> msgFieldsAndTypes        = null;

    //////Storm
    private OutputCollector                         _collector;

    //////Country
    private String                                  country                  = null;

    public BoltForMonitor(String country) {
        this.country = country;
    }

    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
                        OutputCollector collector) {
        //prepare mysql fields for clean & update
        initForMySQL("/properties/monitor/" + country.toLowerCase() + "/mysql-metis-monitor.properties");
        
        //prepare msg fields and their types
        initForMsg("/properties/monitor/" + country.toLowerCase() + "/msg-metis-monitor.properties");

        _collector = collector;
    }

    public void initForMsg(String msgConf) {
        //msg config
        Properties properties = new Properties();
        try {
            properties.load(BoltForMonitor.class.getResourceAsStream(msgConf));
        } catch (IOException e) {
            throw new RuntimeException("Msg properties file load error.", e);//用RuntimeException去中断程序运行
        }

        ////Msg Fields Type
        msgFieldsAndTypes = new HashMap<Integer, Entry<String, String>>();

        for (Entry<Object, Object> ent : properties.entrySet()) {
            Integer fieldIndex = Integer.valueOf(((String) ent.getKey()).replaceFirst("msg.input.",
                    ""));
            String[] fieldNameAndType = ((String) ent.getValue()).split(":");
            if (fieldNameAndType.length == 2) {
                String fieldName = fieldNameAndType[0];
                String fieldType = fieldNameAndType[1];
                msgFieldsAndTypes.put(fieldIndex, new AbstractMap.SimpleEntry<String, String>(
                        fieldName, fieldType));
            }
        }
    }

    public void initForMySQL(String mysqlConf) {
        //load mysql config
        Properties properties = new Properties();
        try {
            properties.load(BoltForMonitor.class.getResourceAsStream(mysqlConf));
        } catch (IOException e) {
            throw new RuntimeException("MySQL properties file load error.", e);//用RuntimeException去中断程序运行
        }

        ////For TDDL
        tableName = properties.getProperty("tddl.table.name");
        tgds = new TGroupDataSource();
        tgds.setAppName(properties.getProperty("tddl.app.name"));
        tgds.setDbGroupKey(properties.getProperty("tddl.db.group.key"));
        try {
            tgds.init();
        } catch (TddlException e) {
            LOGGER.error(String.format("[Bolt][TDDL init error] %s", e));
            throw new RuntimeException(String.format("[Bolt][TDDL init error] %s", e));
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

    //////////////////////////////////////////////////////////////////////////
    //Manual Change Data In Msg & Process The New Msg
    //针对branch为ALL的情况
    public HashMap<String, Object> manualChangeMsg(HashMap<String, Object> msgKVs,
                                                   HashMap<String, Object> manualKVs) {
        msgKVs.putAll(manualKVs);
        return msgKVs;
    }

    //////////////////////////////////////////////////////////////////////////
    ////For Clean
    //过滤msg中的kv, 只保留msg key 与 mysql field 一致的那些msg kv
    public ArrayList<Object> msgKVsMatchMySQLFieldsForClean(HashMap<String, Object> msgKVs,
                                                            HashMap<String, String> fieldsForSet,
                                                            ArrayList<String> fieldsForEqual,
                                                            ArrayList<String> fieldsForNotEqual) {
        LOGGER.warn(String.format("[Clean][fieldsForSet.size:%d]"
                + "[fieldsForEqual.size:%d][fieldsForNotEqual.size:%d]", fieldsForSet.size(),
                fieldsForEqual.size(), fieldsForNotEqual.size()));
        LOGGER.warn(String.format("[Clean][msgKVs.size:%d]", msgKVs.size()));

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

    public HashMap<String, Object> manualChangeMsgKVs(HashMap<String, Object> msgKVs) {
        File tempFile = null;
        try {
            tempFile = StreamFileUtils.stream2file(BoltForMonitor.class
                    .getResourceAsStream("/groovy/monitor/ManualForMonitor.groovy"));
        } catch (IOException e) {
            throw new RuntimeException("Groovy file load error.", e);//用RuntimeException去中断程序运行
        }
        return ManualTask.runForMonitor(tempFile, msgKVs, country);
    }

    public HashMap<String, Object> getMsgKVsUseProperties(Tuple tuple) {
        HashMap<String, Object> msgKVs = new HashMap<String, Object>();

        for (int i = 0; i < tuple.size(); i++) {
            Entry<String, String> fieldNameAndType = msgFieldsAndTypes.get(i);
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

    public HashMap<String, Object> getMsgKVsUseProperties(List<Object> tuple) {
        HashMap<String, Object> msgKVs = new HashMap<String, Object>();

        LOGGER.debug(String.format(
                "[getMsgKVsUseProperties][tuple.size:%d][msgFieldsAndTypes.size:%d]", tuple.size(),
                msgFieldsAndTypes.size()));

        for (int i = 0; i < tuple.size(); i++) {
            Entry<String, String> fieldNameAndType = msgFieldsAndTypes.get(i);
            String fieldName = fieldNameAndType.getKey();
            String fieldType = fieldNameAndType.getValue();

            Object fieldValue = null;
            if (fieldType.equals("string")) {
                fieldValue = (String) tuple.get(i);
            } else if (fieldType.equals("int")) {
                fieldValue = (Integer) tuple.get(i);
            } else if (fieldType.equals("long")) {
                fieldValue = (Long) tuple.get(i);
            } else if (fieldType.equals("float")) {
                fieldValue = (Float) tuple.get(i);
            } else if (fieldType.equals("double")) {
                fieldValue = (Double) tuple.get(i);
            } else if (fieldType.equals("boolean")) {
                fieldValue = (Boolean) tuple.get(i);
            }
            msgKVs.put(fieldName, fieldValue);
        }
        return msgKVs;
    }

    ///1. 实时流量监控monitor
    public void monitorMySQL(ArrayList<Object> valsForClean,//For Clean
                             HashMap<String, String> setFields, ArrayList<String> equalFields,
                             ArrayList<String> notEqualFields,
                             ArrayList<Object> valsForUpdate,//For Update
                             ArrayList<String> fieldsForSet, ArrayList<String> fieldsForSum,
                             ArrayList<String> fieldsForMax) {
        monitorCleanMySQL(valsForClean, setFields, equalFields, notEqualFields);
        monitorUpdateMySQL(valsForUpdate, fieldsForSet, fieldsForSum, fieldsForMax);
    }

    ///1.1 实时流量监控monitor：清理48h前的数据
    public void monitorCleanMySQL(ArrayList<Object> valsForClean,
                                  HashMap<String, String> setFields, ArrayList<String> equalFields,
                                  ArrayList<String> notEqualFields) {
        //把相同segmentId的，但不是最近15min内而是48h前的消息清空
        //UPDATE guider_quark_monitor_rt_cn 
        //SET ts=?,
        //max_concurrent=0,exec_count=0,recovery_count=0,
        //zero_result_count=0,response_time=0,oceanus_queue_time=0,oceanus_cpu_cost
        //WHERE ts_segment_id=? AND scene_id=? AND scene_branch=? AND ts<>?" 
        try { 
            Connection conn = tgds.getConnection();
             
            PreparedStatement psClean = PreparedStatementUtils.getCleanPreparedStatement(conn,
                    tableName, setFields, equalFields, notEqualFields);

            LOGGER.debug(String.format("[monitorCleanMySQL][valsForClean.size:%d]",
                    valsForClean.size()));
            for (int i = 0; i < valsForClean.size(); i++) {
                Object val = valsForClean.get(i);
                int psLoc = i + 1;//PreparedStatement的下标从1开始，而非从零开始
                System.out.println("[psLoc][" + psLoc + "][" + val);
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
            LOGGER.warn(String.format("[res = psClean.executeUpdate()]%d", res));

            if (psClean != null)
                psClean.close();
            if(conn != null)
                conn.close();
        } catch (SQLException e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String fullInfo = sw.toString();
                LOGGER.warn(String.format("[Bolt][MySQL clean]%s", fullInfo));
            return;
        }
    }

    ///1.2 实时流量监控monitor：用Msg的数据更新MySQL
    public void monitorUpdateMySQL(ArrayList<Object> valsForUpdate, ArrayList<String> fieldsForSet,
                                   ArrayList<String> fieldsForSum, ArrayList<String> fieldsForMax) {
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
        try {
            Connection conn = tgds.getConnection();
             
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
            LOGGER.warn(String.format("[res = psReplace.executeUpdate()]%d", res));

            if (psReplace != null)
                psReplace.close();

            if(conn != null)
                conn.close();
        } catch (SQLException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String fullInfo = sw.toString();
            LOGGER.warn(String.format("[Bolt][updateMySQL][TDDL replace: %s]", fullInfo));
        }
    }

    public void msgToMySQL(HashMap<String, Object> msgKVs) {
        /////////////////////////////////////////////////////////       
        //1. 过滤得到MySQL需要的Vals
        ArrayList<Object> msgValsForClean = msgKVsMatchMySQLFieldsForClean(msgKVs,
                setFieldsForClean, equalFieldsForClean, notEqualFieldsForClean);
        ArrayList<Object> msgValsForUpdate = msgKVsMatchMySQLFieldsForUpdate(msgKVs,
                setFieldsForUpdate, sumFieldsForUpdate, maxFieldsForUpdate);

        LOGGER.debug(String.format("[msgToMySQL]"
                + "[msgKVs.size:%d][msgValsForClean.size:%d][msgValsForUpdate.size:%d]",
                msgKVs.size(), msgValsForClean.size(), msgValsForUpdate.size()));

        /////////////////////////////////////////////////////////
        //2. 将消息中的Vals按规则更新到MySQL的fields中       
        monitorMySQL(msgValsForClean, setFieldsForClean, equalFieldsForClean,
                notEqualFieldsForClean, msgValsForUpdate, setFieldsForUpdate, sumFieldsForUpdate,
                maxFieldsForUpdate);
    }

    public void execute(Tuple tuple) {
        try {
            LOGGER.warn(String.format("[tuple] %s", tuple.toString()));
            //time,scene_id,scene_branch,max_concurrent,exec_count,recovery_count,zero_result_count,response_time,oceanus_queue_time,oceanus_cost_cpu
            //[2015-10-14 19:02:00, 50042, b1, 1, 13, 0, 0, 190, 4, 8]

            int size = tuple.size();
            if (size < 10) {
                LOGGER.warn(String.format("[tuple size error] %s", tuple.toString()));
                _collector.ack(tuple);
                return;
            }

            long t1 = System.currentTimeMillis();

            //1. 解析Tuple得到KV,然后更新scene_branch: b1, b2, ...
            //HashMap<String, Object> msgKVs = getMsgKVs(tuple);
            HashMap<String, Object> msgKVs = getMsgKVsUseProperties(tuple);
            msgKVs = manualChangeMsgKVs(msgKVs);
            LOGGER.warn(String.format("[execute][msgKVs:%s]", msgKVs));
            msgToMySQL(msgKVs);

            //2. 更新scene_branch: ALL
            if (setFieldsForMaunalChange != null && setFieldsForMaunalChange.size() > 0) {
                HashMap<String, Object> msgKVsForBranchAll = manualChangeMsg(msgKVs,
                        setFieldsForMaunalChange);
                msgToMySQL(msgKVsForBranchAll);
            }

            long t2 = System.currentTimeMillis();
            LOGGER.debug(String.format("[ cost ][mixTDDLAndTuple %s ms]", String.valueOf(t2 - t1)));

        } catch (Throwable t) {
            StringWriter sw = new StringWriter();
            t.printStackTrace(new PrintWriter(sw));
            String fullInfo = sw.toString();

            if (tuple != null)
                LOGGER.error(String.format("[Catch Throwable Error][tuple:%s][error:%s]",
                        tuple.toString(), fullInfo));
            else
                LOGGER.error("[Catch Throwable Error][tuple is null][error:%s]", fullInfo);
            _collector.ack(tuple);
            return;
        }
        _collector.ack(tuple);
    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Map getComponentConfiguration() {
        return null;
    }
}
