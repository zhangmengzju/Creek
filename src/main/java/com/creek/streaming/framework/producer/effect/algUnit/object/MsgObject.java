package com.creek.streaming.framework.producer.effect.algUnit.object;

import java.util.HashMap;

public class MsgObject {
//ae msg
//+-----------+--------------+---------------------------------------------------+----------------+
//|MsgTypePart|HBaseJoinPart |                  MemKeyPart1                      |  MemValPart1   |
//+-----------+--------------+------------+----------------+-------+-------------+----------------+
//|    "ae"   |"requestId"=""|"sceneId"=""|"sceneBranch"=""|"ts"=""|"tsSegId"="3"|"MsgItemNum1"=1L|   
//+-----------+--------------+------------+----------------+-------+-------------+----------------+

//metis msg
//+-----------+--------------+-----------------------------+-------------------+
//|MsgTypePart|HBaseJoinPart |          MemKeyPart2        |    MemValPart2    |
//+-----------+--------------+-----------------------------+-------------------+
//|   "metis" |"requestId"=""|                             | "MsgItemNum2"= 18L| 
//+-----------+--------------+-----------------------------+-------------------+

    private String                    msgTypePart   = null; // "ae" or "metis"
    
    private HashMap<String, String>   hbaseJoinPart = null; // "request_id"="xxx"

    ///////msg中要组成key的那些部分，作为内存中与MySQL关联的HashMap的key的一部分
    //{(sceneId=50001), (sceneBranch=b1), (ts=2015-06-09 19:15:00), (tsSegId=102)}
    private HashMap<String, String>    memKeyPart   = null;

    ///////msg中要组成val的那些部分，作为内存中与MySQL关联的HashMap的val的一部分
    private HashMap<String, Long>      memValPart   = null;

    public MsgObject(String msgTypePart, 
                     HashMap<String, String> hbaseJoinPart,
                     HashMap<String, String> memKeyPart, 
                     HashMap<String, Long> memValPart) {
        super();
        this.msgTypePart = msgTypePart;
        this.hbaseJoinPart = hbaseJoinPart;
        this.memKeyPart = memKeyPart;
        this.memValPart = memValPart;
    }

    public String getMsgTypePart() {
        return msgTypePart;
    }

    public void setMsgTypePart(String msgTypePart) {
        this.msgTypePart = msgTypePart;
    }

    public HashMap<String, String> getHBaseJoinPart() {
        return hbaseJoinPart;
    }

    public void setHBaseJoinPart(HashMap<String, String> hbaseJoinPart) {
        this.hbaseJoinPart = hbaseJoinPart;
    }

    public HashMap<String, String> getMemKeyPart() {
        return memKeyPart;
    }

    public void setMemKeyPart(HashMap<String, String> memKeyPart) {
        this.memKeyPart = memKeyPart;
    }

    public HashMap<String, Long> getMemValPart() {
        return memValPart;
    }

    public void setMemValPart(HashMap<String, Long> memValPart) {
        this.memValPart = memValPart;
    }

    @Override
    public String toString() {
        return "MsgObject [msgTypePart=" + msgTypePart + ", HBaseJoinPart=" + hbaseJoinPart
                + ", MemKeyPart=" + memKeyPart + ", MemValPart=" + memValPart + "]";
    }     
}
