package com.creek.streaming.framework.producer.effect.sceneBranch;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creek.streaming.framework.utils.StreamFileUtils;

public class ScriptInfo {
    private static final Logger                           LOGGER                   = LoggerFactory
                                                                                           .getLogger(ScriptInfo.class);
    private static GroovyClassLoader                      loader                   = new GroovyClassLoader();
    public static ConcurrentHashMap<String, GroovyObject> scriptNameToGroovyObject = new ConcurrentHashMap<String, GroovyObject>();

    public static void initForScript(String scriptInfoFile) throws IOException {
        //msg config
        Properties properties = new Properties();
        properties.load(ScriptInfo.class.getResourceAsStream(scriptInfoFile));        
        
        if (properties.containsKey("boltExecutorScript")) {
            LOGGER.error("[boltExecutorScript]" + properties.getProperty("boltExecutorScript"));
            try {
                String boltExecutorScriptText = FileUtils.readFileToString(StreamFileUtils
                        .stream2file(ScriptInfo.class.getResourceAsStream(properties
                                .getProperty("boltExecutorScript"))));
                LOGGER.error("[boltExecutorScriptText]" + boltExecutorScriptText); 
                GroovyObject boltExecutorGroovyObject = (GroovyObject) loader.parseClass(
                        boltExecutorScriptText).newInstance();
                if(boltExecutorGroovyObject == null)
                    LOGGER.error("[error:boltExecutorScriptText -> boltExecutorGroovyObject]"); 
                else
                    LOGGER.error("[error:boltExecutorGroovyObject]" + boltExecutorGroovyObject.toString()); 
                    
                scriptNameToGroovyObject.put("boltExecutorScript", boltExecutorGroovyObject);
            } catch (Exception e) {
                throw new RuntimeException("boltExecutorScriptText load error.", e);//用RuntimeException去中断程序运行
            }
        }
        LOGGER.error("[after boltExecutorScript][scriptNameToGroovyObject.size]" + scriptNameToGroovyObject.size()
                + "[scriptNameToGroovyObject.get(boltExecutorScript)]" + scriptNameToGroovyObject.get("boltExecutorScript"));
        
        if (properties.containsKey("filterMetisMsgScript")) {
            LOGGER.error("[filterMetisMsgScript]" + properties.getProperty("filterMetisMsgScript"));
            try {
                String filterMetisMsgScriptText = FileUtils.readFileToString(StreamFileUtils
                        .stream2file(ScriptInfo.class.getResourceAsStream(properties
                                .getProperty("filterMetisMsgScript"))));
                GroovyObject filterMetisMsgGroovyObject = (GroovyObject) loader.parseClass(
                        filterMetisMsgScriptText).newInstance();
                scriptNameToGroovyObject.put("filterMetisMsgScript", filterMetisMsgGroovyObject);
            } catch (Exception e) {
                throw new RuntimeException("filterMetisMsgScriptText load error.", e);//用RuntimeException去中断程序运行
            }
        }
        LOGGER.error("[after filterMetisMsgScript][scriptNameToGroovyObject.size]" + scriptNameToGroovyObject.size()
                + "[scriptNameToGroovyObject.get(filterMetisMsgScript)]" + scriptNameToGroovyObject.get("filterMetisMsgScript"));

        if (properties.containsKey("filterAEMsgScript")) {
            LOGGER.error("[filterAEMsgScript]" + properties.getProperty("filterAEMsgScript"));
            try {
                String filterAEMsgScriptText = FileUtils.readFileToString(StreamFileUtils
                        .stream2file(ScriptInfo.class.getResourceAsStream(properties
                                .getProperty("filterAEMsgScript"))));
                GroovyObject filterAEMsgGroovyObject = (GroovyObject) loader.parseClass(
                        filterAEMsgScriptText).newInstance();
                scriptNameToGroovyObject.put("filterAEMsgScript", filterAEMsgGroovyObject);
            } catch (Exception e) {
                throw new RuntimeException("filterAEMsgScriptText load error.", e);//用RuntimeException去中断程序运行
            }
        }
        LOGGER.error("[after filterAEMsgScript][scriptNameToGroovyObject.size]" + scriptNameToGroovyObject.size()
                + "[scriptNameToGroovyObject.get(filterAEMsgScript)]" + scriptNameToGroovyObject.get("filterAEMsgScript"));

        if (properties.containsKey("enrichMetisMsgScript")) {
            LOGGER.error("[enrichMetisMsgScript]" + properties.getProperty("enrichMetisMsgScript"));
            try {
                String enrichMetisMsgScriptText = FileUtils.readFileToString(StreamFileUtils
                        .stream2file(ScriptInfo.class.getResourceAsStream(properties
                                .getProperty("enrichMetisMsgScript"))));
                GroovyObject enrichMetisMsgGroovyObject = (GroovyObject) loader.parseClass(
                        enrichMetisMsgScriptText).newInstance();
                scriptNameToGroovyObject.put("enrichMetisMsgScript", enrichMetisMsgGroovyObject);
            } catch (Exception e) {
                throw new RuntimeException("enrichMetisMsgScriptText load error.", e);//用RuntimeException去中断程序运行
            }
        }
        LOGGER.error("[after enrichMetisMsgScript][scriptNameToGroovyObject.size]" + scriptNameToGroovyObject.size()
                + "[scriptNameToGroovyObject.get(enrichMetisMsgScript)]" + scriptNameToGroovyObject.get("enrichMetisMsgScript"));

        if (properties.containsKey("enrichAEMsgScript")) {
            LOGGER.error("[enrichAEMsgScript]" + properties.getProperty("enrichAEMsgScript"));
            try {
                String enrichAEMsgScriptText = FileUtils.readFileToString(StreamFileUtils
                        .stream2file(ScriptInfo.class.getResourceAsStream(properties
                                .getProperty("enrichAEMsgScript"))));
                GroovyObject enrichAEMsgGroovyObject = (GroovyObject) loader.parseClass(
                        enrichAEMsgScriptText).newInstance();
                scriptNameToGroovyObject.put("enrichAEMsgScript", enrichAEMsgGroovyObject);
            } catch (Exception e) {
                throw new RuntimeException("enrichAEMsgScriptText load error.", e);//用RuntimeException去中断程序运行
            }
        }
        LOGGER.error("[after enrichAEMsgScript][scriptNameToGroovyObject.size]" + scriptNameToGroovyObject.size()
                + "[scriptNameToGroovyObject.get(enrichAEMsgScript)]" + scriptNameToGroovyObject.get("enrichAEMsgScript"));

        if (properties.containsKey("getMemValScript")) {
            LOGGER.error("[getMemValScript]" + properties.getProperty("getMemValScript"));
            try {
                String getMemValScriptText = FileUtils.readFileToString(StreamFileUtils
                        .stream2file(ScriptInfo.class.getResourceAsStream(properties
                                .getProperty("getMemValScript"))));
                GroovyObject getMemValGroovyObject = (GroovyObject) loader.parseClass(
                        getMemValScriptText).newInstance();
                scriptNameToGroovyObject.put("getMemValScript", getMemValGroovyObject);
            } catch (Exception e) {
                throw new RuntimeException("getMemValScriptText load error.", e);//用RuntimeException去中断程序运行
            }
        }
        LOGGER.error("[after getMemValScript][scriptNameToGroovyObject.size]" + scriptNameToGroovyObject.size()
                + "[scriptNameToGroovyObject.get(getMemValScript)]" + scriptNameToGroovyObject.get("getMemValScript"));

    }
}
