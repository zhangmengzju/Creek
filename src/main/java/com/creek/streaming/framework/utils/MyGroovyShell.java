package com.creek.streaming.framework.utils;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyGroovyShell {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyGroovyShell.class);
    
    private Binding binding = new Binding();
        
    public Object getProperty(String name) {
        return binding.getProperty(name);
    }
     
    public void setParameters(String[] paramNames, Object[] paramValues) {
        int len = paramNames.length;
        if (len != paramValues.length) {
            LOGGER.error("[GroovyScriptEngine][setParameters]parameters not match!");
        }
     
        for (int i = 0; i < len; i++) {
            binding.setProperty(paramNames[i], paramValues[i]);
        }
    }
     
    public Object runScriptUseFileName(String scriptName) {
        GroovyShell shell = new GroovyShell(binding);
        try {
            return shell.evaluate(new File(scriptName));
        } catch (Exception e) {
            LOGGER.error("[GroovyScriptEngine][runScript][error]" + scriptName);
            return null;
        }
    }
     
    public Object runScript(File scriptFile) {
        GroovyShell shell = new GroovyShell(binding);
        try {
            long t1 = System.currentTimeMillis();
            Object obj = shell.evaluate(scriptFile);
            long t2 = System.currentTimeMillis();
            LOGGER.error(String.format("[GroovyScriptEngine][runScript][%s ms]",
                    String.valueOf(t2 - t1)));
            return obj;
            
        } catch (Exception e) {
            LOGGER.error("[GroovyScriptEngine][runScript][error]"
                    + "[" + scriptFile+"]"
                    + "[" + e + "]");
            return null;
        }
    }        
       
    public Object runScript(String scriptContent) {
        GroovyShell shell = new GroovyShell(binding);
        try {
            long t1 = System.currentTimeMillis();
            Object obj = shell.evaluate(scriptContent);
            long t2 = System.currentTimeMillis();
            LOGGER.error(String.format("[GroovyScriptEngine][runScript][%s ms]",
                    String.valueOf(t2 - t1)));
            return obj;
            
        } catch (Exception e) {
            LOGGER.error("[GroovyScriptEngine][runScript][error]"
                    + "[" + scriptContent+"]"
                    + "[" + e + "]");
            return null;
        }
    }  
    
    public static void main(String[] args) {
            
    }
}
