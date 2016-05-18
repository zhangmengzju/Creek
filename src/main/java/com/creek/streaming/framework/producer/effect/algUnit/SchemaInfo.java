package com.creek.streaming.framework.producer.effect.algUnit;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaInfo {
    private static final Logger                            LOGGER                  = LoggerFactory
                                                                                           .getLogger(SchemaInfo.class);
    //////Msg Fields And Types
    private static HashMap<Integer, Entry<String, String>> msgSchemaForMetis       = null;
    private static HashMap<Integer, Entry<String, String>> msgSchemaForAE          = null;

    private static Entry<String, String>                   schemaForHBaseJoin      = null;

    private static HashMap<Integer, Entry<String, String>> schemaForMetisForMemKey = null;
    private static HashMap<Integer, Entry<String, String>> schemaForAEForMemKey    = null;

    private static Entry<String, String>                   schemaForMetisForMemVal = null;
    private static Entry<String, String>                   schemaForAEForMemVal    = null;

    private static HashSet<String>                         aeMemKeyPartFields      = new HashSet<String>();
    private static HashSet<String>                         metisMemKeyPartFields   = new HashSet<String>();

    private static HashSet<String>                         memKeyFields            = new HashSet<String>();
    private static HashSet<String>                         memValFields            = new HashSet<String>();

    public static void initForSchema(String msgConf) throws IOException {
        HashMap<Integer, Entry<String, String>> msgInputSchema = new HashMap<Integer, Entry<String, String>>();
        HashMap<Integer, Entry<String, String>> schemaForMemKey = new HashMap<Integer, Entry<String, String>>();
        Entry<String, String> schemaForMemVal = null;

        //msg config
        Properties properties = new Properties();
        properties.load(RecreateMsg.class.getResourceAsStream(msgConf));
        
        for (Entry<Object, Object> ent : properties.entrySet()) {
            String key = (String) ent.getKey();

            ///1. msg input schema
            if (key.startsWith("msg.input.")) {
                Integer fieldIndex = Integer.valueOf(key.replaceFirst("msg.input.", ""));
                String[] fieldNameAndType = ((String) ent.getValue()).split(":");
                if (fieldNameAndType.length == 2) {
                    msgInputSchema.put(fieldIndex, new AbstractMap.SimpleEntry<String, String>(
                            fieldNameAndType[0], fieldNameAndType[1]));
                }
            }

            ///2. msg output schema : hbase join part
            else if (key.equals("msg.output.hbase.join.part")) {
                String[] fieldNameAndType = ((String) ent.getValue()).split(":");
                if (fieldNameAndType.length == 2) {
                    schemaForHBaseJoin = new AbstractMap.SimpleEntry<String, String>(
                            fieldNameAndType[0], fieldNameAndType[1]);
                }
            }

            ///3. msg output schema : MemKey Part
            else if (key.startsWith("msg.output.mem.key.part.")) {
                Integer fieldIndex = Integer.valueOf(key.replaceFirst("msg.output.mem.key.part.",
                        ""));
                String[] fieldNameAndType = ((String) ent.getValue()).split(":");
                if (fieldNameAndType.length == 2) {
                    schemaForMemKey.put(fieldIndex, new AbstractMap.SimpleEntry<String, String>(
                            fieldNameAndType[0], fieldNameAndType[1]));
                }
            }

            ///4. msg output schema : MemVal Part
            else if (key.equals("msg.output.mem.val.part")) {//=item_click_count:long
                String[] fieldNameAndType = ((String) ent.getValue()).split(":");
                if (fieldNameAndType.length == 2) {
                    schemaForMemVal = new AbstractMap.SimpleEntry<String, String>(
                            fieldNameAndType[0], fieldNameAndType[1]);
                }
            }
        }

        LOGGER.error("[initForMsg][msgInputSchema:" + msgInputSchema + "]" + 
                "[schemaForHBaseJoin:" + schemaForHBaseJoin + "]" + 
                "[schemaForMemKey:" + schemaForMemKey + "]" + 
                "[schemaForMemVal:" + schemaForMemVal + "]");

        if (msgConf.contains("ae")) {
            msgSchemaForAE = msgInputSchema;
            schemaForAEForMemKey = schemaForMemKey;
            schemaForAEForMemVal = schemaForMemVal;

            // aeMemKeyPartFields
            if (schemaForAEForMemKey != null) {
                for (Entry<String, String> ent : schemaForAEForMemKey.values()) {
                    String key = ent.getKey();
                    if (key != null && key.length() > 0)
                        aeMemKeyPartFields.add(key);
                }
                memKeyFields.addAll(aeMemKeyPartFields);
            }
            
            // aeMemValPartFields
            if (schemaForAEForMemVal != null) {
                memValFields.add(schemaForAEForMemVal.getKey());
            }
        } else if (msgConf.contains("metis")) {
            msgSchemaForMetis = msgInputSchema;
            schemaForMetisForMemKey = schemaForMemKey;
            schemaForMetisForMemVal = schemaForMemVal;

            // metisMemKeyPartFields
            if (schemaForMetisForMemKey != null) {
                for (Entry<String, String> ent : schemaForMetisForMemKey.values()) {
                    String key = ent.getKey();
                    if (key != null && key.length() > 0)
                        metisMemKeyPartFields.add(key);
                }
                memKeyFields.addAll(metisMemKeyPartFields);    
            }
            
            // metisMemValPartFields 
            if (schemaForMetisForMemVal != null) {
                memValFields.add(schemaForMetisForMemVal.getKey());
            }
        }
    }

    public static HashMap<Integer, Entry<String, String>> getMsgSchemaForMetis() {
        return msgSchemaForMetis;
    }

    public static void setMsgSchemaForMetis(HashMap<Integer, Entry<String, String>> msgSchemaForMetis) {
        SchemaInfo.msgSchemaForMetis = msgSchemaForMetis;
    }

    public static HashMap<Integer, Entry<String, String>> getMsgSchemaForAE() {
        return msgSchemaForAE;
    }

    public static void setMsgSchemaForAE(HashMap<Integer, Entry<String, String>> msgSchemaForAE) {
        SchemaInfo.msgSchemaForAE = msgSchemaForAE;
    }

    public static Entry<String, String> getSchemaForHBaseJoin() {
        return schemaForHBaseJoin;
    }

    public static HashMap<Integer, Entry<String, String>> getSchemaForMetisForMemKey() {
        return schemaForMetisForMemKey;
    }

    public static HashMap<Integer, Entry<String, String>> getSchemaForAEForMemKey() {
        return schemaForAEForMemKey;
    }

    public static Entry<String, String> getSchemaForMetisForMemVal() {
        return schemaForMetisForMemVal;
    }

    public static Entry<String, String> getSchemaForAEForMemVal() {
        return schemaForAEForMemVal;
    }

    public static HashSet<String> getAeMemKeyPartFields() {
        return aeMemKeyPartFields;
    }

    public static HashSet<String> getMetisMemKeyPartFields() {
        return metisMemKeyPartFields;
    }

    public static HashSet<String> getMemKeyFields() {
        return memKeyFields;
    }

    public static HashSet<String> getMemValFields() {
        return memValFields;
    }
}
