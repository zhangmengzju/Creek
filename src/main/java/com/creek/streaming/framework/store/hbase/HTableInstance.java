package com.creek.streaming.framework.store.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTableInstance {
    //////Log
    private static final Logger LOGGER = LoggerFactory.getLogger(HTableInstance.class);
    private HTable              table  = null;

    public HTableInstance(HTable table) {
        this.table = table;
    }
    
    public void put(String rowKey, String family, 
                    HashMap<String, String> curMemKeyPart, 
                    HashMap<String, Long> curMemValPart) {
        if(rowKey == null || family == null 
                || (curMemKeyPart == null) && (curMemValPart == null) ) 
            return;
        
        //一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        Put put = new Put(rowKey.getBytes());

        if(curMemKeyPart != null){
            for(Entry<String, String> ent : curMemKeyPart.entrySet()){
                put.add(family.getBytes(), ent.getKey().getBytes(), Bytes.toBytes(ent.getValue()));             
            }
        }

        if(curMemValPart != null){
            for(Entry<String, Long> ent : curMemValPart.entrySet()){
                put.add(family.getBytes(), ent.getKey().getBytes(), Bytes.toBytes(ent.getValue()));             
            }
        }
        
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "[HBase put fail][rowKey: %s][family: %s][curMemKeyPart: %s][curMemValPart: %s][IOException:%s]",
                    rowKey, family, curMemKeyPart, curMemValPart, e));
        }
    }    
    
    public void put(String rowKey, String family, HashMap<String, String> colNameAndColVal) {
        if(rowKey == null || family == null || colNameAndColVal == null)
            return;
        
        //一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        Put put = new Put(rowKey.getBytes());

        for(Entry<String, String> ent : colNameAndColVal.entrySet()){
            put.add(family.getBytes(), ent.getKey().getBytes(), Bytes.toBytes(ent.getValue()));             
        }

        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                            "[HBase put fail][rowKey: %s][family: %s][colNameAndColVal: %s][IOException:%s]",
                            rowKey, family, colNameAndColVal, e));
        }
    }    
    
    public void put(String rowKey, String family, String qualifier, Object value) {
        if(rowKey == null || family == null || qualifier == null || value == null)
            return;
        
        //一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        Put put = new Put(rowKey.getBytes());

        if (value instanceof Integer) {
            put.add(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((Integer) value));
        } else if (value instanceof String) {
            put.add(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((String) value));
        } else if (value instanceof Double) {
            put.add(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((Double) value));
        } else if (value instanceof Long) {
            put.add(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((Long) value));
        } else if (value instanceof Float) {
            put.add(family.getBytes(), qualifier.getBytes(), Bytes.toBytes((Float) value));
        } else {
            LOGGER.error(String.format("[unsupport data type]%s", value.getClass().getName()));
            return;
        }

        try {
            table.put(put);           
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "[HBase put fail][rowKey: %s][family: %s][qualifier: %s][value: %s][IOException:%s]",
                    rowKey, family, qualifier, value, e));
        }
    }
    
    public long incrementColumnValue(String rowKey,
                                     String family,
                                     String qualifier,
                                     long amount) {
        long newVal = -1L;
        try {
            newVal = table.incrementColumnValue(rowKey.getBytes(), family.getBytes(), qualifier.getBytes(), amount);
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "[HBase incrementColumnValue fail][rowKey: %s][family: %s][qualifier: %s][amount: %s][IOException:%s]",
                    rowKey, family, qualifier, amount, e));
        }
        return newVal;
    }
    
    public byte[] get(String rowKey, String family, String qualifier) {
        Get g = new Get(Bytes.toBytes(rowKey));
        g.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

        try {
            return table.get(g).getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "[HBase get fail][rowKey: %s][family: %s][qualifier: %s][IOException:%s]",
                    rowKey, family, qualifier, e));
        }
    }

    public HashMap<String, byte[]> get(String rowKey, String family, HashSet<String> colNameSet) {
        HashMap<String, byte[]> res = new HashMap<String, byte[]>();
        
        Get g = new Get(Bytes.toBytes(rowKey));        
        for(String colName : colNameSet){
            g.addColumn(Bytes.toBytes(family), Bytes.toBytes(colName));
        }

        Result queRes = null;
        try {
            queRes = table.get(g);
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                "[HBase get fail][rowKey: %s][family: %s][colNameSet: %s][IOException:%s]",
                rowKey, family, colNameSet, e));
        }
        for(KeyValue kv : queRes.raw()){
            String colStr = Bytes.toString(kv.getQualifier());
            byte[] val = kv.getValue();
            if(colNameSet.contains(colStr))
                res.put(colStr, val);
        }
        return res;
    }
    
    //add row for many column  
    public void addRow(String rowKey, String family, HashMap<String, String> columnAndValue) {
        Put p = new Put(Bytes.toBytes(rowKey));
        //参数出分别：列族、列、值  
        for (String column : columnAndValue.keySet()) {
            String value = columnAndValue.get(column);
            p.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        }
        try {
            table.put(p);
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "[HBase addRow fail][rowKey: %s][family: %s][columnAndValue: %s][IOException:%s]",
                    rowKey, family, columnAndValue, e));
        }
    }

    public String[] getColumnsInColumnFamily(Result r, String columnFamily) {
        NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap(Bytes.toBytes(columnFamily));
        String[] Quantifers = new String[familyMap.size()];

        int counter = 0;
        for (byte[] bQunitifer : familyMap.keySet()) {
            Quantifers[counter++] = Bytes.toString(bQunitifer);
        }

        return Quantifers;
    }

    //get multi column name and their value , in format : Map<columnName,columnValue>  
    public HashMap<String, byte[]> getRow(String rowKey, String family) {
        HashMap<String, byte[]> res = new HashMap<String, byte[]>();
        Get g = new Get(Bytes.toBytes(rowKey));
        Result row = null;
        try {
            row = table.get(g);
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "[HBase getRow fail][rowKey: %s][family: %s][IOException:%s]",
                    rowKey, family, e));
        }

        NavigableMap<byte[], byte[]> familyMap = row.getFamilyMap(Bytes.toBytes(family));
        if(familyMap != null){
            for (byte[] colNameBytes : familyMap.keySet()) {
                String colName = Bytes.toString(colNameBytes);
                byte[] colValue = familyMap.get(colNameBytes);
                res.put(colName, colValue);
            }
        }
        return res;
    }

    public void delete(String rowKey) {
        Delete d = new Delete(Bytes.toBytes(rowKey));
        try {
            table.delete(d);
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "[HBase delete fail][rowKey: %s][IOException:%s]", rowKey, e));
        }
    }

    public ResultScanner scan(String prefix) {
        byte[] prefixKey = prefix.getBytes();

        Scan scan = new Scan();
        scan.setStartRow(prefixKey);
        scan.setStopRow(Bytes.add(prefixKey, new byte[] { (byte) 0xff }));

        ResultScanner resScanner = null;
        try {
            resScanner = table.getScanner(scan);
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "[HBase scan fail][prefix: %s][IOException:%s]", prefix, e));
        }
         
        return resScanner;
    }

    public void updateHBaseCategoryCountData(String rowKey, String family, 
                                             String qualifier, int deltaCount) {
        //标志HBase中是否存在该rowkey
        boolean rowkeyExistFlag = false;

        int cateCountBefore = 0;
        byte[] cateCount = get(rowKey, family, qualifier);
        if (cateCount != null) {
            rowkeyExistFlag = true;
            cateCountBefore = Bytes.toInt(cateCount);
            LOGGER.debug(String.format("[updateHBaseCategoryCountData][cateCountBefore %d][deltaCount %d]]",
                    cateCountBefore, deltaCount));
        } 

        int cateCountAfter = cateCountBefore + deltaCount;
        if (cateCountAfter > 0)
            put(rowKey, family, qualifier, cateCountAfter);//使用范型来进行支持Int->Bytes,String->Bytes等的转换
        else if (rowkeyExistFlag == true)
            delete(rowKey);
    }
}
