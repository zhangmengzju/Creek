package com.creek.streaming.framework.producer.monitor;

import java.io.File;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creek.streaming.framework.utils.MyGroovyShell;

public class ManualTask {
    //////Log
    private static final Logger LOGGER                 = LoggerFactory.getLogger(ManualTask.class);

    public static final int    TIME_1_MIN_IN_MS      = 1 * 60 * 1000;
    public static final int    TIME_15_MIN_IN_MS      = 15 * 60 * 1000;
    public static final int    RECORD_NUM_IN_48_HOURS = 48 * (60 / 15);

    public static HashMap<String, Object> runForMonitor(File scriptFile, HashMap<String, Object> msgKVs,
                                              String country) {
        MyGroovyShell manTask = new MyGroovyShell();
        String[] paramNames = { "msgKVs", "country" };
        Object[] paramValues = { msgKVs, country };
        manTask.setParameters(paramNames, paramValues);

        @SuppressWarnings("unchecked")
        HashMap<String, Object> res = (HashMap<String, Object>) manTask.runScript(scriptFile);
        LOGGER.debug(String.format("[ManualTask][res:%s]", res));
        return res;
    }
    
    
    public static HashMap<String, Object> runForEffect(File scriptFile, HashMap<String, Object> msgKVs,
            String country) {
        MyGroovyShell manTask = new MyGroovyShell();
        String[] paramNames = { "msgKVs", "country" };
        Object[] paramValues = { msgKVs, country };
        manTask.setParameters(paramNames, paramValues);

        @SuppressWarnings("unchecked")
        HashMap<String, Object> res = (HashMap<String, Object>) manTask.runScript(scriptFile);
        LOGGER.debug(String.format("[ManualTask][res:%s]", res));
        return res;
    }

    public static void main(String[] args) {
        ////ScriptPath
        String sp = "src/main/resources/groovy/monitor/ManualForMonitor.groovy";
        ////msgKVs
        HashMap<String, Object> msgKVs = new HashMap<String, Object>();
        msgKVs.put("time", "2015-10-14 19:09:00");
        ////country
        String country = "us";

        HashMap<String, Object> msgKVsNew = ManualTask.runForMonitor(new File(sp), msgKVs, country);
        System.out.println("[msgKVsNew]" + msgKVsNew);
    }
}
