package com.creek.streaming.framework.utils;

import groovy.lang.GroovyObject;

import java.io.IOException;
import java.util.HashMap; 

import org.codehaus.groovy.control.CompilationFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.creek.streaming.framework.producer.effect.algUnit.ScriptInfo;
 
public class MyGroovyClassLoader {
    public static final Logger LOGGER = LoggerFactory.getLogger(MyGroovyClassLoader.class);   

    public Object runScript(String scriptName, Object[] args) throws CompilationFailedException,
            IOException, InstantiationException, IllegalAccessException {
        long t1 = System.currentTimeMillis();
        if((ScriptInfo.scriptNameToGroovyObject == null)||
                (ScriptInfo.scriptNameToGroovyObject.containsKey(scriptName)==false)){
           
            LOGGER.error("[*MyGroovyClassLoader*]"
                    + "[scriptName:"+ scriptName + "]"
                    + "[args.size:" + args.length + "]"
                    + "[ScriptInfo.scriptNameToGroovyObject.size()" + ScriptInfo.scriptNameToGroovyObject.size() + "]"
                    + "[ScriptInfo.scriptNameToGroovyObject]" + ScriptInfo.scriptNameToGroovyObject
                    + "[ScriptInfo.scriptNameToGroovyObject.keySet()]" + ScriptInfo.scriptNameToGroovyObject.keySet()
                    + "[ScriptInfo.scriptNameToGroovyObject.values()]" + ScriptInfo.scriptNameToGroovyObject.values());
            
            return null;
        }
            
        GroovyObject groObj = ScriptInfo.scriptNameToGroovyObject.get(scriptName);      
        Object res = groObj.invokeMethod("manualChangeMsgKVs", args);
        long t2 = System.currentTimeMillis();
        LOGGER.debug(String.format("[*cost*][MyGroovyClassLoader][%s ms]", String.valueOf(t2 - t1)));
        return res;
    }

    public static void main(String[] args) throws CompilationFailedException,
            InstantiationException, IllegalAccessException, IOException {
        ////ScriptPath
        String sp = "src/main/resources/groovy/effect/sceneBranch/enrichAEMsgScript.groovy";
        ////msgKVs
        HashMap<String, Object> msgKVs = new HashMap<String, Object>();
        msgKVs.put(
                "args",
                "referPageId=008f391f-f97d-390d-9c45-dd4e8d14702b,pageFrom=native,pageId=c0fcf57d-897c-347c-99f6-8be1c127b6a5,_rid=20151023070859376-1425277688,productId=2036073430,spm-cnt=a1z65.Detail.0.0,isbk=1,spm-url=a1z65.Home.0.0,dep=727");
        ////country
        String country = "us";

        MyGroovyClassLoader manTask = new MyGroovyClassLoader();
        Object[] argsNew = { msgKVs, country };
        manTask.runScript(sp, argsNew);
    }
}
