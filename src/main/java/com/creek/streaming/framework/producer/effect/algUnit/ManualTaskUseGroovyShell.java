package com.creek.streaming.framework.producer.effect.algUnit;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

import com.creek.streaming.framework.utils.MyGroovyShell;

public class ManualTaskUseGroovyShell implements Serializable {
    private static final long  serialVersionUID       = -8761858989824920749L;
    public static final Logger LOGGER                 = LoggerFactory
                                                              .getLogger(ManualTaskUseGroovyShell.class);

    public static final int    TIME_1_MIN_IN_MS       = 1 * 60 * 1000;
    public static final int    TIME_15_MIN_IN_MS      = 15 * 60 * 1000;
    public static final int    RECORD_NUM_IN_48_HOURS = 48 * (60 / 15);

    public static HashMap<String, Object> enrichMsgKVs(String scriptText,//File scriptFile,
                                                       HashMap<String, Object> msgKVs,
                                                       String country) throws IOException {
        MyGroovyShell manTask = new MyGroovyShell();
        Object[] args = {msgKVs, country};
        String[] paramNames = {"args"};//"msgKVs", "country" };
        Object[] paramValues = {args};//msgKVs, country };
        
        manTask.setParameters(paramNames, paramValues);

        long t1 = System.currentTimeMillis();
        //HashMap<String, Object> res = (HashMap<String, Object>) manTask.runScript(scriptFile);
        @SuppressWarnings("unchecked")
        HashMap<String, Object> res = (HashMap<String, Object>) manTask.runScript(scriptText);
        long t2 = System.currentTimeMillis();
        LOGGER.error(String.format("[*cost*][bolt][%s ms]", String.valueOf(t2 - t1)));

        LOGGER.debug(String.format("[ManualTaskForEffect][res:%s]", res));
        return res;
    }

    public static ArrayList<HashMap<String, Object>> enrichMsgKVsNew(String scriptFileText,
                                                                     HashMap<String, Object> msgKVs,
                                                                     String country) {
        MyGroovyShell manTask = new MyGroovyShell();
        Object[] args = {msgKVs, country};
        String[] paramNames = {"args"};//"msgKVs", "country" };
        Object[] paramValues = {args};//msgKVs, country };
        manTask.setParameters(paramNames, paramValues);

        @SuppressWarnings("unchecked")
        ArrayList<HashMap<String, Object>> res = (ArrayList<HashMap<String, Object>>) manTask
                .runScript(scriptFileText);
        LOGGER.debug(String.format("[ManualTaskForEffect][res:%s]", res));
        return res;
    }

    public static boolean filterMsgKVs(String scriptFileText, HashMap<String, Object> msgKVs) {
        MyGroovyShell manTask = new MyGroovyShell();
        Object[] args = {msgKVs};
        String[] paramNames = {"args"};//"msgKVs"
        Object[] paramValues = {args};//msgKVs
        manTask.setParameters(paramNames, paramValues);

        boolean res = (Boolean) manTask.runScript(scriptFileText);
        LOGGER.debug(String.format("[ManualTaskForEffect][res:%s]", res));
        return res;
    }

    public static long getMemVal(String scriptFileText, HashMap<String, Object> msgKVs, String msgTypePart) {
        MyGroovyShell manTask = new MyGroovyShell();
        Object[] args = {msgKVs, msgTypePart};
        String[] paramNames = {"args"};//"msgKVs", "msgTypePart" };
        Object[] paramValues = {args};//msgKVs, msgTypePart };
        manTask.setParameters(paramNames, paramValues);

        long res = (Long) manTask.runScript(scriptFileText);
        LOGGER.debug(String.format("[ManualTaskForEffect][res:%s]", res));
        return res;
    }

    public static long boltExecute(String scriptFileText, Tuple tuple, RecreateMsg rm, JoinMsgToMem jmtm,
                                   MemToMySQL mtm, AggregateMsgInMem amim, long lastUpdateMySQLTime) {
        MyGroovyShell manTask = new MyGroovyShell();
        Object[] args = {tuple, rm, jmtm, mtm, amim, lastUpdateMySQLTime};
        String[] paramNames = {"args"};//"tuple", "rm", "jmtm", "mtm", "amim", "lastUpdateMySQLTime" 
        Object[] paramValues = {args};//tuple, rm, jmtm, mtm, amim, lastUpdateMySQLTime
        
        manTask.setParameters(paramNames, paramValues);
        LOGGER.debug(String.format("[ManualTaskForEffect][boltExecute][scriptFileText:%s]", scriptFileText));
        long res = (Long) manTask.runScript(scriptFileText);
        LOGGER.debug(String.format("[ManualTaskForEffect][res:%s]", res));
        
        if(res == -1L)
            return lastUpdateMySQLTime;
        else
            return res;
    }

    //////////////////test/////////////////////////////////////////////////////////
    public static void testMetisScript() throws IOException {
        ////ScriptPath
        String sp = "src/main/resources/groovy/effect/algUnit/enrichMetisMsgScript.groovy";
        ////msgKVs
        HashMap<String, Object> msgKVs = new HashMap<String, Object>();
        msgKVs.put("time", "2015-10-14 19:09:00");
        ////country
        String country = "us";

        String fileText = FileUtils.readFileToString(new File(sp));
        HashMap<String, Object> msgKVsNew = ManualTaskUseGroovyShell.enrichMsgKVs(fileText, msgKVs,
                country);
        System.out.println("[msgKVsNew]" + msgKVsNew);
    }

    public static void testAEScript() throws IOException {
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
            HashMap<String, Object> msgKVsNew = ManualTaskUseGroovyShell.enrichMsgKVs(fileText, msgKVs,
                    country);
            System.out.println("[msgKVsNew]" + msgKVsNew);
        }

    }

    public static void testFilterScript() throws IOException {
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
        boolean flag = ManualTaskUseGroovyShell.filterMsgKVs(t, msgKVs);
        System.out.println("[msgKVsNew]" + flag);
    }

    public static void testGetMemValScript() throws IOException {
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
        long memVal = ManualTaskUseGroovyShell.getMemVal(t, msgKVs, "metis");
        System.out.println("[msgKVsNew]" + memVal);
    }

    public static void main(String[] args) throws IOException {
        //testMetisScript();
        //testAEScript();
        //testFilterScript(); 
        //testGetMemValScript();
    }
}
