package com.creek.streaming.framework.producer.effect.sceneBranch;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.codehaus.groovy.control.CompilationFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creek.streaming.framework.utils.MyGroovyClassLoader;

public class ManualTaskUseGroovyClassLoader implements Serializable {
    private static final long  serialVersionUID       = -8761858989824920749L;
    public static final Logger LOGGER                 = LoggerFactory
                                                              .getLogger(ManualTaskUseGroovyClassLoader.class);

    public static final int    TIME_1_MIN_IN_MS       = 1 * 60 * 1000;
    public static final int    TIME_15_MIN_IN_MS      = 15 * 60 * 1000;
    public static final int    RECORD_NUM_IN_48_HOURS = 48 * (60 / 15);

    public static HashMap<String, Object> enrichMsgKVs(String scriptName,//File scriptFile,
                                                       HashMap<String, Object> msgKVs,
                                                       String country) throws IOException, CompilationFailedException, InstantiationException, IllegalAccessException {     
        MyGroovyClassLoader manTask = new MyGroovyClassLoader();
        Object[] args = {msgKVs, country}; 

        long t1 = System.currentTimeMillis(); 
        @SuppressWarnings("unchecked")
        HashMap<String, Object> res = (HashMap<String, Object>) manTask.runScript(scriptName, args);
        long t2 = System.currentTimeMillis();
        LOGGER.debug(String.format("[*cost*][bolt][%s ms][res:%s]", String.valueOf(t2 - t1), res));
        return res;
    }

    public static ArrayList<HashMap<String, Object>> enrichMsgKVsNew(String scriptName,
                                                                     HashMap<String, Object> msgKVs,
                                                                     String country) throws CompilationFailedException, InstantiationException, IllegalAccessException, IOException {
        MyGroovyClassLoader manTask = new MyGroovyClassLoader();
        Object[] args = {msgKVs, country};

        @SuppressWarnings("unchecked")
        ArrayList<HashMap<String, Object>> res = (ArrayList<HashMap<String, Object>>) manTask
                .runScript(scriptName, args);
        LOGGER.debug(String.format("[ManualTaskForEffect][res:%s]", res));
        return res;
    }

    public static boolean filterMsgKVs(String scriptName, HashMap<String, Object> msgKVs) throws CompilationFailedException, InstantiationException, IllegalAccessException, IOException {
        MyGroovyClassLoader manTask = new MyGroovyClassLoader();
        Object[] args = {msgKVs}; 
        
        boolean res = (Boolean) manTask.runScript(scriptName, args);
        LOGGER.debug(String.format("[ManualTaskForEffect][res:%s]", res));
        return res;
    }

    public static long getMemVal(String scriptName, HashMap<String, Object> msgKVs, String msgTypePart) throws CompilationFailedException, InstantiationException, IllegalAccessException, IOException {
        MyGroovyClassLoader manTask = new MyGroovyClassLoader();
        Object[] args = {msgKVs, msgTypePart}; 
        
        long res = (Long) manTask.runScript(scriptName, args);
        LOGGER.debug(String.format("[ManualTaskForEffect][res:%s]", res));
        return res;
    }
    
    public static long boltExecute(String scriptName, Object[] args) throws CompilationFailedException, InstantiationException, IllegalAccessException, IOException {
        
        LOGGER.error("[**][ScriptInfo.scriptNameToGroovyObject.size()]" + ScriptInfo.scriptNameToGroovyObject.size());
        
        MyGroovyClassLoader manTask = new MyGroovyClassLoader();               
        Object res = manTask.runScript(scriptName, args);
        if((res == null) || (-1 == (Long)res)){
            LOGGER.error("[*boltExecute*][scriptName:" + scriptName + "][res:" + res + "]");
            return -1L;
        } else {
            LOGGER.error("[*boltExecute*][scriptName:" + scriptName + "][res:" + ((Long)res) + "]");
            return (Long)res;
        }   
    }

    //////////////////test/////////////////////////////////////////////////////////
    public static void testMetisScript() throws IOException, CompilationFailedException, InstantiationException, IllegalAccessException {
        ////ScriptPath
        String sp = "src/main/resources/groovy/effect/algUnit/enrichMetisMsgScript.groovy";
        ////msgKVs
        HashMap<String, Object> msgKVs = new HashMap<String, Object>();
        msgKVs.put("time", "2015-10-14 19:09:00");
        ////country
        String country = "us";
 
        String fileText = FileUtils.readFileToString(new File(sp));
        HashMap<String, Object> msgKVsNew = ManualTaskUseGroovyClassLoader.enrichMsgKVs(fileText, msgKVs,
                country);
        System.out.println("[msgKVsNew]" + msgKVsNew);
    }

    public static void testAEScript() throws IOException, CompilationFailedException, InstantiationException, IllegalAccessException {
        ////ScriptPath
        String sp = "src/main/resources/groovy/effect/algUnit/enrichAEMsgScript.groovy";
        ////msgKVs
        HashMap<String, Object> msgKVs = new HashMap<String, Object>();
        msgKVs.put(
                "args",
                "referPageId=008f391f-f97d-390d-9c45-dd4e8d14702b,pageFrom=native,pageId=c0fcf57d-897c-347c-99f6-8be1c127b6a5,_rid=20151023070859376-1425277688,productId=2036073430,spm-cnt=a1z65.Detail.0.0,isbk=1,spm-url=a1z65.Home.0.0,dep=727");
        ////country
        String country = "us";

        String fileText = FileUtils.readFileToString(new File(sp));
        for (int i = 0; i < 150; i++) {
            HashMap<String, Object> msgKVsNew = ManualTaskUseGroovyClassLoader.enrichMsgKVs(fileText, msgKVs,
                    country);
            System.out.println("[msgKVsNew]" + msgKVsNew);
        }

    }

    public static void testFilterScript() throws IOException, CompilationFailedException, InstantiationException, IllegalAccessException {
        /*
         * ///metis ///ScriptPath String sp =
         * "src/main/resources/groovy/effect/filterMetisMsgScript.groovy";
         * ////msgKVs HashMap<String, Object> msgKVs = new HashMap<String,
         * Object>(); msgKVs.put("item_list", "32460743137" + "\003" +
         * "sceneHotSol" + "\004" + "1760675706" + "\003" + "sceneHotSol");
         * msgKVs.put("scene_id", "10002"); msgKVs.put("scene_branch", "b1");
         * msgKVs.put("time", "2015-10-22 09:11:04"); msgKVs.put("request_id",
         * "20151022090001654-1846552184"); boolean flag =
         * ManualTaskForEffect.filterMsgKVs(new File(sp), msgKVs);
         * System.out.println("[msgKVsNew]" + flag);
         */

        ////ae
        ////ScriptPath
        String sp = "src/main/resources/groovy/effect/filterAEMsgScript.groovy";
        ////msgKVs
        HashMap<String, Object> msgKVs = new HashMap<String, Object>();
        msgKVs.put("log_time", "2015-10-23 07:09:31");
        msgKVs.put("page_type", "DETAIL");
        msgKVs.put(
                "args",
                "referPageId=008f391f-f97d-390d-9c45-dd4e8d14702b,pageFrom=native,pageId=c0fcf57d-897c-347c-99f6-8be1c127b6a5,_rid=20151023070859376-1425277688,productId=2036073430,spm-cnt=a1z65.Detail.0.0,isbk=1,spm-url=a1z65.Home.0.0,dep=727");
        File f = new File(sp);
        String t = FileUtils.readFileToString(f);
        boolean flag = ManualTaskUseGroovyClassLoader.filterMsgKVs(t, msgKVs);
        System.out.println("[msgKVsNew]" + flag);
    }

    public static void testGetMemValScript() throws IOException, CompilationFailedException, InstantiationException, IllegalAccessException {
        ////ScriptPath
        String sp = "src/main/resources/groovy/effect/getMemValScript.groovy";
        ////msgKVs
        HashMap<String, Object> msgKVs = new HashMap<String, Object>();
        msgKVs.put("item_list", "32460743137" + "\003" + "sceneHotSol" + "\004" + "1760675706"
                + "\003" + "sceneHotSol");
        msgKVs.put("scene_id", "10002");
        msgKVs.put("scene_branch", "b1");
        msgKVs.put("time", "2015-10-22 09:11:04");
        msgKVs.put("request_id", "20151022090001654-1846552184");

        File f = new File(sp);
        String t = FileUtils.readFileToString(f);
        long memVal = ManualTaskUseGroovyClassLoader.getMemVal(t, msgKVs, "metis");
        System.out.println("[msgKVsNew]" + memVal);
    }

    public static void main(String[] args) throws IOException {
        //testMetisScript();
        //testAEScript();
        //testFilterScript(); 
        //testGetMemValScript();
    }
}
