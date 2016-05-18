package com.creek.streaming.framework.producer.monitor;

import com.creek.streaming.framework.utils.MyGroovyShell;

public class ProducerTask {
   
    public static void run(){
        ///Producer Part
        MyGroovyShell producer = new MyGroovyShell();
        
        for(int i=11 ; i<18; i++){
            String[] paramNames = { "producerId" };
            Object[] paramValues = { new Integer(i) };
            producer.setParameters(paramNames, paramValues);
        
            producer.runScript("src/main/resources/groovy/Producer.groovy");
        }
    }
}
