package com.creek.streaming.framework.consumer;

import java.util.LinkedHashSet;

import com.creek.streaming.framework.utils.MyGroovyShell;

public class ConsumerTask implements Runnable {
    private LinkedHashSet<Long>           activeUserSet;
    
    MyGroovyShell consumer = new MyGroovyShell();
    
    public ConsumerTask(LinkedHashSet<Long> aus) {
        this.activeUserSet = aus;
    }
    
    public void run() {
        while(true) {
            long tmpId = -1L;
            
            synchronized (activeUserSet) {
                //在LinkedHashSet中没数据时，consumer线程要wait   
                while (activeUserSet.size() == 0) {  
                    try {
                        activeUserSet.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }            
                
                //在LinkedHashSet中有数据时，consumer线程去消费LinkedHashSet中元素         
                tmpId = activeUserSet.iterator().next();
                activeUserSet.remove(tmpId);             
            }
            
            //consumer线程的具体处理逻辑（放在groovy文件中，用户可配置）
            if (tmpId > 0) {
                String[] consumerParamNames = { "tmpId" };
                Object[] consumerParamValues = { new Long(tmpId) };
                consumer.setParameters(consumerParamNames, consumerParamValues);
                Object consumerResult = consumer.runScript("src/main/resources/groovy/Consumer.groovy");
                System.out.println(consumerResult);       
            }             
        }    
    }
}
