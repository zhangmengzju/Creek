package com.creek.streaming.framework.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreparedStatementUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(PreparedStatementUtils.class);

    public static String assembleFields(int tsSegmentId, String ts, int sceneId,
                                        String sceneBranch, int maxConcurrent, int execCount,
                                        int recoveryCount, int zeroResultCount, int responseTime,
                                        int oceanusQueueTime, int oceanusCPUCost) {
        StringBuilder sb = new StringBuilder();
        sb.append("[ts_segment_id->" + tsSegmentId + "]");
        sb.append("[ts->" + ts + "]");
        sb.append("[scene_id->" + sceneId + "]");
        sb.append("[scene_branch->" + sceneBranch + "]");
        sb.append("[max_concurrent->" + maxConcurrent + "]");
        sb.append("[exec_count->" + execCount + "]");
        sb.append("[recovery_count->" + recoveryCount + "]");
        sb.append("[zero_result_count->" + zeroResultCount + "]");
        sb.append("[response_time->" + responseTime + "]");
        sb.append("[oceanus_queue_time->" + oceanusQueueTime + "]");
        sb.append("[oceanus_cpu_cost->" + oceanusCPUCost + "]");
        return sb.toString();
    }
   
    ///////////////////////////////////////////////////////////////////////////////
    //getReplacePreparedStatement
    ///////////////////////////////////////////////////////////////////////////////
    public static PreparedStatement getCleanPreparedStatement(Connection conn,
                                                              String tableName,
                                                              HashMap<String, String> fieldsForSet,
                                                              List<String> fieldsForEuqalCondition,
                                                              List<String> fieldsForNotEuqalCondition) throws SQLException {
        //"UPDATE " + tableName + " SET ts=?,item_exp_count_s=0,item_click_count=0 "
        //"WHERE ts_segment_id=? AND scene_id=? AND scene_branch=? AND ts<>?"
        if (fieldsForSet == null)
            fieldsForSet = new HashMap<String, String>();
        if (fieldsForEuqalCondition == null)
            fieldsForEuqalCondition = new ArrayList<String>();
        if (fieldsForNotEuqalCondition == null)
            fieldsForNotEuqalCondition = new ArrayList<String>();

        //set部分不能为空
        int setNum = fieldsForSet.size();
        if (setNum == 0) {
            return null;
        }

        //condition部分的equal部分不能为空，否则会清空几乎全部数据
        int equConNum = fieldsForEuqalCondition.size();
        if (equConNum == 0) {
            return null;
        }

        String setPartSQL = "UPDATE " + tableName + getSetPartSQLStr(fieldsForSet);
        String conditionPartSQL = getConditionPartSQLStr(fieldsForEuqalCondition,
                fieldsForNotEuqalCondition);

        String fullSQL = setPartSQL + conditionPartSQL;
        LOGGER.debug(String.format("[Clean][fullSQL]%s", fullSQL));

        return conn.prepareStatement(fullSQL);
    }
    
    public static String getSetPartSQLStr(HashMap<String, String> fieldsForSet) {
        if (fieldsForSet == null || fieldsForSet.size() == 0) {
            return null;
        }

        //"SET ts=? ,item_exp_count_s=0,item_click_count=0 " 
        String res = " SET ";
        int i = 0;
        for (String fieldName : fieldsForSet.keySet()) {
            if (i == 0) {
                res = res + fieldName + " = " + fieldsForSet.get(fieldName);
            } else {
                res = res + ", " + fieldName + " = " + fieldsForSet.get(fieldName);
            }
            i++;
        }
        return res;
    }

    public static String getConditionPartSQLStr(List<String> fieldsForEuqalCondition,
                                                List<String> fieldsForNotEuqalCondition) {
        if (fieldsForEuqalCondition == null || fieldsForEuqalCondition.size() == 0) {
            return null;
        }

        //"WHERE ts_segment_id=? AND scene_id=? AND scene_branch=?" 
        String res = " WHERE ";
        for (int i = 0; i < fieldsForEuqalCondition.size(); i++) {
            if (i == 0)
                res = res + fieldsForEuqalCondition.get(i) + " = ? ";
            else
                res = res + " AND " + fieldsForEuqalCondition.get(i) + " = ?";
        }

        //"AND ts<>?"
        for (int i = 0; i < fieldsForNotEuqalCondition.size(); i++) {
            res = res + " AND " + fieldsForNotEuqalCondition.get(i) + " <> ?";
        }

        return res;
    }   

    ///////////////////////////////////////////////////////////////////////////////
    //getReplacePreparedStatement
    ///////////////////////////////////////////////////////////////////////////////
    public static PreparedStatement getReplacePreparedStatement(Connection conn, String tableName,
                                                                List<String> fieldsForInsert,
                                                                List<String> fieldsForSum,
                                                                List<String> fieldsForMax) throws SQLException {
        ///fieldsForInsert应包含：fieldsForSum,fieldsForMax和其他fields
        if (fieldsForInsert == null)
            fieldsForInsert = new ArrayList<String>();
        if (fieldsForSum == null)
            fieldsForSum = new ArrayList<String>();
        if (fieldsForMax == null)
            fieldsForMax = new ArrayList<String>();

        int insertNum = fieldsForInsert.size();
        int sumNum = fieldsForSum.size();
        int maxNum = fieldsForMax.size();

        String insertPartSQL = "INSERT INTO " + tableName;
        String sumPartSQL = "";
        String maxPartSQL = "";

        //1. insertPartSQL
        //""INSERT INTO " + tableName + " (ts_segment_id,ts,scene_id,scene_branch,item_exp_count_s,item_click_count) "
        //" VALUES (?,?,?,?,?,?) "        
        //2. sumPartSQL
        //" item_exp_count_s = item_exp_count_s + ?, "
        //" item_click_count = item_click_count + ? "               
        //3. maxPartSQL
        //" , max_xxx = GREATEST(max_xxx, ?) "        

        if (insertNum == 0) {
            return null;
        } else {
            insertPartSQL += getInsertPartSQLStr(fieldsForInsert);

            if (sumNum == 0 && maxNum == 0) {
                sumPartSQL = "";
                maxPartSQL = "";
            } else if (sumNum == 0 && maxNum > 0) {
                sumPartSQL = " ON DUPLICATE KEY UPDATE ";
                maxPartSQL = getUpdatePartSQLStr(fieldsForMax, " = GREATEST(", ", ?)");
            } else if (sumNum > 0 && maxNum == 0) {
                sumPartSQL = " ON DUPLICATE KEY UPDATE "
                        + getUpdatePartSQLStr(fieldsForSum, " = ", " + ? ");
                maxPartSQL = "";
            } else {
                sumPartSQL = " ON DUPLICATE KEY UPDATE "
                        + getUpdatePartSQLStr(fieldsForSum, " = ", " + ? ");
                maxPartSQL = ", " + getUpdatePartSQLStr(fieldsForMax, " = GREATEST(", ", ?)");
            }
        }
        String fullSQL = insertPartSQL + sumPartSQL + maxPartSQL;
        LOGGER.debug(String.format("[Replace][fullSQL]%s", fullSQL));
        
        return conn.prepareStatement(fullSQL);        
    }
    
    public static String getInsertPartSQLStr(List<String> fields) {
        //" (ts_segment_id,ts,scene_id,scene_branch,item_exp_count_s,item_click_count) "
        if (fields == null || fields.size() == 0)
            return null;

        String res = " ";
        for (int i = 0; i < fields.size(); i++) {
            if (i == 0)
                res = res + "(" + fields.get(i);
            else
                res = res + "," + fields.get(i);
        }
        res += ")";

        //" VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE "        
        for (int i = 0; i < fields.size(); i++) {
            if (i == 0)
                res = res + " VALUES (?";
            else
                res = res + ",?";
        }
        res += ")";

        return res;
    }

    public static String getUpdatePartSQLStr(List<String> fields, String prefix, String suffix) {
        if (fields == null || fields.size() == 0)
            return null;

        String res = "";
        for (int i = 0; i < fields.size(); i++) {
            if (i == 0)
                res = res + fields.get(i) + prefix + fields.get(i) + suffix;
            else
                res = res + ", " + fields.get(i) + prefix + fields.get(i) + suffix;
        }
        return res;
    }
    ///////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////

    public static String assembleFields(int tsSegmentId, String ts, int sceneId,
                                        String sceneBranch, int itemExpCountS, int itemClickCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("[ts_segment_id->" + tsSegmentId + "]");
        sb.append("[ts->" + ts + "]");
        sb.append("[scene_id->" + sceneId + "]");
        sb.append("[scene_branch->" + sceneBranch + "]");
        sb.append("[item_exp_count_s->" + itemExpCountS + "]");
        sb.append("[item_click_count->" + itemClickCount + "]");
        return sb.toString();
    }

    public static void main(String[] args) throws SQLException {
        /////////////////////////////////////////////////////////////
        //Test Replace SQL
        //        List<String> fieldsForSet = new ArrayList<String>();
        //        fieldsForSet.add("col1");
        //        fieldsForSet.add("col2");
        //        fieldsForSet.add("col3");
        //        //ts_segment_id,ts,scene_id,scene_branch,item_exp_count_s,item_click_count
        //        
        //        
        //        //        List<String> fieldsForSum = null;
        //        List<String> fieldsForSum = new ArrayList<String>();
        //        fieldsForSum.add("col4");
        ////        fieldsForSum.add("col5");
        ////        fieldsForSum.add("col6");
        ////       
        ////        List<String> fieldsForMax = null;
        //        List<String> fieldsForMax = new ArrayList<String>();
        //        fieldsForMax.add("col7");
        //        fieldsForMax.add("col8");
        //        fieldsForMax.add("col9");
        //        
        //        getReplacePreparedStatement(null,"mytable", fieldsForSet, fieldsForSum, fieldsForMax);

        /////////////////////////////////////////////////////////////
        //Test Clean SQL
        HashMap<String, String> fieldsForSet = new HashMap<String, String>();
        fieldsForSet.put("col1", "?");
        //        fieldsForSet.put("col2", "0");
        //        fieldsForSet.put("col3", "?");

        List<String> fieldsForEuqalCondition = new ArrayList<String>();
        fieldsForEuqalCondition.add("col4");
        fieldsForEuqalCondition.add("col5");

        List<String> fieldsForNotEuqalCondition = new ArrayList<String>();
        fieldsForNotEuqalCondition.add("col6");
        fieldsForNotEuqalCondition.add("col7");

        getCleanPreparedStatement(null, "mytable", fieldsForSet, fieldsForEuqalCondition,
                fieldsForNotEuqalCondition);
    }
}
