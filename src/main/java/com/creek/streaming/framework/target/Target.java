package com.creek.streaming.framework.target;

import com.creek.streaming.framework.utils.MyGroovyShell;

public class Target {
    
    public static void init(){
        ///Store Part
        MyGroovyShell target = new MyGroovyShell();
        
        for(int i=11 ; i<18; i++){
            String[] paramNames = { "producerId" };
            Object[] paramValues = { new Integer(i) };
            target.setParameters(paramNames, paramValues);
        
            target.runScript("src/main/resources/Store.groovy");
        }
    }
}
