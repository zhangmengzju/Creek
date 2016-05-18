package com.creek.streaming.framework.source;

import java.util.HashMap;

import backtype.storm.topology.IRichSpout;

import com.creek.streaming.framework.utils.MyGroovyShell;

public class Source {
    
    public static HashMap<String, IRichSpout> spouts = new HashMap<String, IRichSpout>();
    
    public static void init(){
        ///Store Part
        MyGroovyShell store = new MyGroovyShell();
        
        for(int i=11 ; i<18; i++){
            String[] paramNames = { "producerId" };
            Object[] paramValues = { new Integer(i) };
            store.setParameters(paramNames, paramValues);
        
            store.runScript("src/main/resources/Store.groovy");
        }
    }
}
